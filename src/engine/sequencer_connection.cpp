#include "engine/sequencer_connection.h"

#include "utils/socket.h"
#include "engine/flags.h"
#include "engine/engine.h"
#include "engine/slog_engine.h"

#define HLOG(l) LOG(l) << "SequencerConnection: "
#define HPLOG(l) PLOG(l) << "SequencerConnection: "
#define HVLOG(l) VLOG(l) << "SequencerConnection: "

namespace faas {
namespace engine {

using protocol::SequencerMessage;
using protocol::SequencerMessageHelper;

SequencerConnection::SequencerConnection(Engine* engine, SLogEngine* slog_engine, int sockfd)
    : ConnectionBase(kTypeId),
      engine_(engine), slog_engine_(slog_engine), state_(kCreated), sockfd_(sockfd) {}

SequencerConnection::~SequencerConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void SequencerConnection::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    if (absl::GetFlag(FLAGS_tcp_enable_nodelay)) {
        CHECK(utils::SetTcpSocketNoDelay(sockfd_));
    }
    if (absl::GetFlag(FLAGS_tcp_enable_keepalive)) {
        CHECK(utils::SetTcpSocketKeepAlive(sockfd_));
    }
    current_io_uring()->PrepareBuffers(kBufGroup, kBufSize);
    URING_DCHECK_OK(current_io_uring()->RegisterFd(sockfd_));
    std::string shared_log_addr = fmt::format("{}:{}", engine_->shared_log_tcp_host(),
                                              engine_->shared_log_tcp_port());
    handshake_message_ = SequencerMessageHelper::NewEngineHandshake(
        engine_->node_id(), shared_log_addr);
    URING_DCHECK_OK(current_io_uring()->SendAll(
        sockfd_, std::span<const char>(reinterpret_cast<const char*>(&handshake_message_),
                                       sizeof(SequencerMessage)),
        [this] (int status) {
            if (status != 0) {
                PLOG(ERROR) << "Failed to send handshake, will close this connection";
            } else {
                HLOG(INFO) << "Handshake done";
                state_ = kRunning;
                URING_DCHECK_OK(current_io_uring()->StartRecv(
                    sockfd_, kBufGroup,
                    absl::bind_front(&SequencerConnection::OnRecvData, this)));
            }
        }
    ));
    state_ = kHandshake;
}

void SequencerConnection::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kHandshake || state_ == kRunning);
    current_io_uring()->StopReadOrRecv(sockfd_);
    URING_DCHECK_OK(current_io_uring()->Close(sockfd_, [this] () {
        DCHECK(state_ == kClosing);
        URING_DCHECK_OK(current_io_uring()->UnregisterFd(sockfd_));
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }));
    state_ = kClosing;
}

void SequencerConnection::SendMessage(const SequencerMessage& message,
                                      std::span<const char> payload) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(WARNING) << "SequencerConnection is closing or has closed, will not send this message";
        return;
    }
    DCHECK_EQ(message.payload_size, gsl::narrow_cast<uint32_t>(payload.size()));
    std::span<char> buf;
    io_worker_->NewWriteBuffer(&buf);
    if (sizeof(SequencerMessage) + payload.size() > buf.size()) {
        HLOG(FATAL) << "Buffer not larger enough! "
                    << "Consider enlarge SequencerConnection::kBufSize";
    }
    memcpy(buf.data(), &message, sizeof(SequencerMessage));
    memcpy(buf.data() + sizeof(SequencerMessage), payload.data(), payload.size());
    URING_DCHECK_OK(current_io_uring()->SendAll(
        sockfd_, std::span<const char>(buf.data(), sizeof(SequencerMessage) + payload.size()),
        [this, buf] (int status) {
            io_worker_->ReturnWriteBuffer(buf);
            if (status != 0) {
                HPLOG(ERROR) << "Failed to send data, will close this connection";
                ScheduleClose();
            }
        }
    ));
}

void SequencerConnection::ProcessSequencerMessages() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    while (read_buffer_.length() >= sizeof(SequencerMessage)) {
        SequencerMessage* message = reinterpret_cast<SequencerMessage*>(read_buffer_.data());
        size_t full_size = sizeof(SequencerMessage) + std::max<size_t>(0, message->payload_size);
        if (read_buffer_.length() >= full_size) {
            std::span<const char> payload(read_buffer_.data() + sizeof(SequencerMessage),
                                          full_size - sizeof(SequencerMessage));
            slog_engine_->OnSequencerMessage(*message, payload);
            read_buffer_.ConsumeFront(full_size);
        } else {
            break;
        }
    }
}

bool SequencerConnection::OnRecvData(int status, std::span<const char> data) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (status != 0) {
        HPLOG(ERROR) << "Read error, will close this connection";
        ScheduleClose();
        return false;
    } else if (data.size() == 0) {
        HLOG(INFO) << "Connection closed remotely";
        ScheduleClose();
        return false;
    } else {
        read_buffer_.AppendData(data);
        ProcessSequencerMessages();
        return true;
    }
}

}  // namespace engine
}  // namespace faas
