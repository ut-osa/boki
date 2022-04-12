#include "engine/message_connection.h"

#include "common/time.h"
#include "ipc/base.h"
#include "ipc/fifo.h"
#include "utils/io.h"
#include "engine/flags.h"
#include "server/constants.h"
#include "engine/engine.h"

namespace faas {
namespace engine {

using protocol::Message;
using protocol::MessageHelper;
using protocol::AuxBufferHeader;

MessageConnection::MessageConnection(Engine* engine, int sockfd)
    : server::ConnectionBase(kMessageConnectionTypeId),
      engine_(engine), io_worker_(nullptr), state_(kCreated),
      func_id_(0), client_id_(0), handshake_done_(false),
      sockfd_(sockfd), pipe_for_write_fd_(-1),
      log_header_("MessageConnection[Handshaking]: ") {
}

MessageConnection::~MessageConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void MessageConnection::Start(server::IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    current_io_uring()->PrepareBuffers(kMessageConnectionBufGroup, kBufSize);
    URING_DCHECK_OK(current_io_uring()->RegisterFd(*sockfd_));
    URING_DCHECK_OK(current_io_uring()->StartRecv(
        *sockfd_, kMessageConnectionBufGroup,
        [this] (int status, std::span<const char> data) -> bool {
            if (status != 0) {
                HPLOG(ERROR) << "Read error on handshake, will close this connection";
                ScheduleClose();
                return false;
            } else if (data.size() == 0) {
                HLOG(INFO) << "Connection closed remotely";
                ScheduleClose();
                return false;
            } else {
                message_buffer_.AppendData(data);
                if (message_buffer_.length() > sizeof(Message)) {
                    HLOG(ERROR) << "Invalid handshake, will close this connection";
                    ScheduleClose();
                    return false;
                } else if (message_buffer_.length() == sizeof(Message)) {
                    RecvHandshakeMessage();
                    return false;
                }
                return true;
            }
        }
    ));
    state_ = kHandshake;
}

void MessageConnection::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kHandshake || state_ == kRunning);
    HLOG(INFO) << "Start closing";
    DCHECK(sockfd_.has_value());
    URING_DCHECK_OK(current_io_uring()->Close(*sockfd_, [this] () {
        sockfd_ = std::nullopt;
        OnFdClosed();
    }));
    if (in_fifo_fd_.has_value()) {
        URING_DCHECK_OK(current_io_uring()->Close(*in_fifo_fd_, [this] () {
            in_fifo_fd_ = std::nullopt;
            OnFdClosed();
        }));
    }
    if (out_fifo_fd_.has_value()) {
        URING_DCHECK_OK(current_io_uring()->Close(*out_fifo_fd_, [this] () {
            out_fifo_fd_ = std::nullopt;
            pipe_for_write_fd_.store(-1);
            OnFdClosed();
        }));
    }
    state_ = kClosing;
}

void MessageConnection::SendPendingMessages() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kHandshake) {
        return;
    }
    if (state_ != kRunning) {
        HLOG(WARNING) << "MessageConnection is closing or has closed, "
                         "will not send pending messages";
        return;
    }
    size_t write_size = 0;
    {
        absl::MutexLock lk(&write_message_mu_);
        write_size = pending_messages_.size() * sizeof(Message);
        if (write_size > 0) {
            write_message_buffer_.Reset();
            write_message_buffer_.AppendData(
                reinterpret_cast<char*>(pending_messages_.data()),
                write_size);
            pending_messages_.clear();
        }
    }
    if (write_size == 0) {
        return;
    }
    size_t n_msg = write_size / sizeof(Message);
    for (size_t i = 0; i < n_msg; i++) {
        const char* ptr = write_message_buffer_.data() + i * sizeof(Message);
        if (out_fifo_fd_.has_value()) {
            const Message* message = reinterpret_cast<const Message*>(ptr);
            if (!WriteMessageWithFifo(*message)) {
                HLOG(FATAL) << "WriteMessageWithFifo failed";
            }
        } else {
            std::span<char> buf;
            io_worker_->NewWriteBuffer(&buf);
            CHECK_GE(buf.size(), sizeof(Message));
            memcpy(buf.data(), ptr, sizeof(Message));
            URING_DCHECK_OK(current_io_uring()->SendAll(
                *sockfd_, std::span<const char>(buf.data(), sizeof(Message)),
                [this, buf] (int status) {
                    io_worker_->ReturnWriteBuffer(buf);
                    if (status != 0) {
                        HPLOG(ERROR) << "Failed to write response, will close this connection";
                        ScheduleClose();
                    }
                }
            ));
        }
    }
}

namespace {
static std::span<const char> CopyToBuffer(std::span<char> buf,
                                          std::span<const char> data) {
    DCHECK_LE(data.size(), buf.size());
    memcpy(buf.data(), data.data(), data.size());
    return std::span<const char>(buf.data(), data.size());
}
}  // namespace

void MessageConnection::SendAuxBufferData() {
    DCHECK(is_func_worker_connection());
    DCHECK(!engine_->func_worker_use_engine_socket());
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kHandshake) {
        return;
    }
    if (state_ != kRunning) {
        HLOG(WARNING) << "MessageConnection is closing or has closed, "
                         "will not send pending data in auxiliary buffer";
        return;
    }
    utils::AppendableBuffer buffer_to_send;
    {
        absl::MutexLock lk(&aux_data_mu_);
        buffer_to_send.Swap(aux_data_buffer_);
    }
    if (buffer_to_send.empty()) {
        return;
    }
    while (!buffer_to_send.empty()) {
        std::span<char> buf;
        io_worker_->NewWriteBuffer(&buf);
        size_t copy_size = std::min(buf.size(), buffer_to_send.length());
        std::span<const char> data(buffer_to_send.data(), copy_size);
        URING_DCHECK_OK(current_io_uring()->SendAll(
            *sockfd_, CopyToBuffer(buf, data),
            [this, buf] (int status) {
                io_worker_->ReturnWriteBuffer(buf);
                if (status != 0) {
                    HPLOG(ERROR) << "Failed to send data, will close this connection";
                    ScheduleClose();
                }
            }
        ));
        buffer_to_send.ConsumeFront(copy_size);
    }
}

void MessageConnection::OnFdClosed() {
    DCHECK(state_ == kClosing);
    if (    !sockfd_.has_value()
         && !in_fifo_fd_.has_value()
         && !out_fifo_fd_.has_value()) {
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }
}

void MessageConnection::RecvHandshakeMessage() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    Message* message = reinterpret_cast<Message*>(message_buffer_.data());
    func_id_ = message->func_id;
    if (MessageHelper::IsLauncherHandshake(*message)) {
        client_id_ = 0;
        log_header_ = fmt::format("LauncherConnection[{}]: ", func_id_);
    } else if (MessageHelper::IsFuncWorkerHandshake(*message)) {
        client_id_ = message->client_id;
        log_header_ = fmt::format("FuncWorkerConnection[{}-{}]: ", func_id_, client_id_);
    } else {
        HLOG(FATAL) << "Unknown handshake message type";
    }
    std::span<const char> payload;
    if (!engine_->OnNewHandshake(this, *message, &handshake_response_, &payload)) {
        ScheduleClose();
        return;
    }
    if (MessageHelper::IsFuncWorkerHandshake(*message)
            && !engine_->func_worker_use_engine_socket()) {
        out_fifo_fd_ = ipc::FifoOpenForWrite(ipc::GetFuncWorkerInputFifoName(client_id_));
        if (!out_fifo_fd_.has_value()) {
            HLOG(ERROR) << "FifoOpenForWrite failed";
            ScheduleClose();
            return;
        }
        URING_DCHECK_OK(current_io_uring()->RegisterFd(*out_fifo_fd_));
        in_fifo_fd_ = ipc::FifoOpenForRead(ipc::GetFuncWorkerOutputFifoName(client_id_));
        if (!in_fifo_fd_.has_value()) {
            HLOG(ERROR) << "FifoOpenForRead failed";
            ScheduleClose();
            return;
        }
        URING_DCHECK_OK(current_io_uring()->RegisterFd(*in_fifo_fd_));
        io_utils::FdUnsetNonblocking(*in_fifo_fd_);
        io_utils::FdUnsetNonblocking(*out_fifo_fd_);
        pipe_for_write_fd_.store(*out_fifo_fd_);
    }
    char* buf = reinterpret_cast<char*>(malloc(sizeof(Message) + payload.size()));
    memcpy(buf, &handshake_response_, sizeof(Message));
    if (payload.size() > 0) {
        memcpy(buf + sizeof(Message), payload.data(), payload.size());
    }
    URING_DCHECK_OK(current_io_uring()->SendAll(
        *sockfd_, std::span<const char>(buf, sizeof(Message) + payload.size()),
        [this, buf] (int status) {
            free(buf);
            if (status != 0) {
                HPLOG(ERROR) << "Failed to write handshake response, will close this connection";
                ScheduleClose();
                return;
            }
            handshake_done_ = true;
            state_ = kRunning;
            message_buffer_.Reset();
            if (in_fifo_fd_.has_value()) {
                URING_DCHECK_OK(current_io_uring()->StartRead(
                    *in_fifo_fd_, kMessageConnectionBufGroup,
                    absl::bind_front(&MessageConnection::OnRecvData, this)));
                URING_DCHECK_OK(current_io_uring()->StartRecv(
                    *sockfd_, kMessageConnectionBufGroup,
                    absl::bind_front(&MessageConnection::OnRecvSockData, this)));
            } else {
                URING_DCHECK_OK(current_io_uring()->StartRecv(
                    *sockfd_, kMessageConnectionBufGroup,
                    absl::bind_front(&MessageConnection::OnRecvData, this)));
            }
            SendPendingMessages();
        }
    ));
}

void MessageConnection::WriteMessage(const Message& message) {
    {
        absl::MutexLock lk(&write_message_mu_);
        pending_messages_.push_back(message);
    }
    io_worker_->ScheduleFunction(
        this, absl::bind_front(&MessageConnection::SendPendingMessages, this));
}

void MessageConnection::WriteAuxBuffer(uint64_t id, std::span<const char> data) {
    DCHECK(is_func_worker_connection());
    if (engine_->func_worker_use_engine_socket()) {
        LOG(FATAL) << "Must not call WriteAuxBuffer "
                   << "when func_worker_use_engine_socket flag is set";
    }
    bool need_schedule_fn = false;
    {
        absl::MutexLock lk(&aux_data_mu_);
        if (aux_data_buffer_.empty()) {
            need_schedule_fn = true;
        }
        std::string hdr = protocol::EncodeAuxBufferHeader(id, data.size());
        aux_data_buffer_.AppendData(STRING_AS_SPAN(hdr));
        aux_data_buffer_.AppendData(data);
    }
    if (need_schedule_fn) {
        io_worker_->ScheduleFunction(
            this, absl::bind_front(&MessageConnection::SendAuxBufferData, this));
    }
}

bool MessageConnection::OnRecvSockData(int status, std::span<const char> data) {
    if (status != 0) {
        HPLOG(ERROR) << "Read error, will close this connection";
        ScheduleClose();
        return false;
    }
    if (data.size() == 0) {
        HLOG(INFO) << "Connection closed remotely";
        ScheduleClose();
        return false;
    }
    received_aux_data_.AppendData(data);
    if (received_aux_data_.length() < sizeof(AuxBufferHeader)) {
        return true;
    }
    const AuxBufferHeader* hdr = reinterpret_cast<const AuxBufferHeader*>(
        received_aux_data_.data());
    if (received_aux_data_.length() >= sizeof(AuxBufferHeader) + hdr->size) {
        std::span<const char> data(
            received_aux_data_.data() + sizeof(AuxBufferHeader),
            hdr->size);
        engine_->OnRecvAuxBuffer(this, hdr->id, data);
        received_aux_data_.ConsumeFront(sizeof(AuxBufferHeader) + hdr->size);
    }
    return true;
}

bool MessageConnection::OnRecvData(int status, std::span<const char> data) {
    if (status != 0) {
        HPLOG(ERROR) << "Read error, will close this connection";
        ScheduleClose();
        return false;
    }
    if (data.size() == 0) {
        if (!in_fifo_fd_.has_value()) {
            HLOG(INFO) << "Connection closed remotely";
            ScheduleClose();
            return false;
        } else {
            return true;
        }
    }
    utils::ReadMessages<Message>(
        &message_buffer_, data.data(), data.size(),
        [this] (Message* message) {
            engine_->OnRecvMessage(this, *message);
        });
    return true;
}

bool MessageConnection::WriteMessageWithFifo(const protocol::Message& message) {
    int fd = pipe_for_write_fd_.load();
    if (fd == -1) {
        return false;
    }
    server::IOWorker* current = server::IOWorker::current();
    if (current == nullptr) {
        return false;
    }
    std::span<char> buf;
    current->NewWriteBuffer(&buf);
    CHECK_GE(buf.size(), sizeof(Message));
    memcpy(buf.data(), &message, sizeof(Message));
    URING_DCHECK_OK(current->io_uring()->Write(
        fd, std::span<const char>(buf.data(), sizeof(Message)),
        [this, current, buf] (int status, size_t nwrite) {
            current->ReturnWriteBuffer(buf);
            if (status != 0 || nwrite == 0) {
                if (status != 0) {
                    HPLOG(ERROR) << "Failed to write message";
                } else {
                    HLOG(ERROR) << "Failed to write message: nwrite=0";
                }
                if (current == io_worker_) {
                    ScheduleClose();
                    return;
                }
            }
            CHECK_EQ(nwrite, sizeof(Message)) << "Write to FIFO is not atomic";
        }
    ));
    return true;
}

}  // namespace engine
}  // namespace faas
