#include "server/egress_hub.h"

#include "common/flags.h"
#include "server/constants.h"

namespace faas {
namespace server {

EgressHub::EgressHub(int type, const struct sockaddr_in* addr, size_t num_conn)
    : ConnectionBase(type),
      io_worker_(nullptr),
      state_(kCreated),
      sockfds_(num_conn, -1),
      log_header_(GetLogHeader(type)),
      send_fn_scheduled_(false) {
    memcpy(&addr_, addr, sizeof(struct sockaddr_in));
}

EgressHub::~EgressHub() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void EgressHub::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    for (size_t i = 0; i < sockfds_.size(); i++) {
        int sockfd = socket(AF_INET, SOCK_STREAM, 0);
        PCHECK(sockfd >= 0) << "Failed to create socket";
        sockfds_[i] = sockfd;
        URING_DCHECK_OK(current_io_uring()->RegisterFd(sockfd));
        URING_DCHECK_OK(current_io_uring()->Connect(
            sockfd, reinterpret_cast<struct sockaddr*>(&addr_), sizeof(addr_),
            absl::bind_front(&EgressHub::OnSocketConnected, this, sockfd)));
    }
    state_ = kRunning;
}

void EgressHub::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    for (int sockfd : sockfds_) {
        if (sockfd >= 0) {
            RemoveSocket(sockfd);
        }
    }
    state_ = kClosing;
}

void EgressHub::SetHandshakeMessageCallback(HandshakeMessageCallback cb) {
    handshake_message_cb_ = cb;
}

void EgressHub::SendMessage(std::span<const char> message) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(ERROR) << "Connection is closing or has closed, will not send this message";
        return;
    }
    if (message.size() == 0) {
        return;
    }
    write_buffer_.AppendData(message);
    ScheduleSendFunction();
}

void EgressHub::SendMessage(std::span<const char> part1, std::span<const char> part2) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(ERROR) << "Connection is closing or has closed, will not send this message";
        return;
    }
    if (part1.size() + part2.size() == 0) {
        return;
    }
    write_buffer_.AppendData(part1);
    write_buffer_.AppendData(part2);
    ScheduleSendFunction();
}

namespace {
static std::span<const char> CopyToBuffer(std::span<char> buf,
                                          std::span<const char> data) {
    DCHECK_LE(data.size(), buf.size());
    memcpy(buf.data(), data.data(), data.size());
    return std::span<const char>(buf.data(), data.size());
}
}

void EgressHub::OnSocketConnected(int sockfd, int status) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (status != 0) {
        HPLOG(ERROR) << "Failed to connect";
        RemoveSocket(sockfd);
        return;
    }
    if (absl::GetFlag(FLAGS_tcp_enable_nodelay)) {
        CHECK(utils::SetTcpSocketNoDelay(sockfd));
    }
    if (absl::GetFlag(FLAGS_tcp_enable_keepalive)) {
        CHECK(utils::SetTcpSocketKeepAlive(sockfd));
    }
    // Setup Recv helps to detect disconnection from the other end
    URING_DCHECK_OK(current_io_uring()->StartRecv(
        sockfd, kOctaBufGroup,
        [this, sockfd] (int status, std::span<const char> data) -> bool {
            if (status != 0) {
                HPLOG(ERROR) << "Read error, will close this connection";
                RemoveSocket(sockfd);
                return false;
            } else if (data.size() == 0) {
                HLOG(INFO) << "Connection closed remotely";
                RemoveSocket(sockfd);
                return false;
            } else {
                HLOG(ERROR) << "Received data on egress sockets, "
                               "this in general should not happen by design";
                return true;
            }
        }
    ));
    std::string handshake;
    if (handshake_message_cb_) {
        handshake_message_cb_(&handshake);
    }
    if (handshake.empty()) {
        SocketReady(sockfd);
        return;
    }
    std::span<char> buf;
    io_worker_->NewWriteBuffer(&buf);
    CHECK_LE(handshake.size(), buf.size());
    URING_DCHECK_OK(current_io_uring()->SendAll(
        sockfd, CopyToBuffer(buf, STRING_TO_SPAN(handshake)),
        [this, sockfd, buf] (int status) {
            io_worker_->ReturnWriteBuffer(buf);
            if (status != 0) {
                HPLOG(ERROR) << "Failed to send handshake";
                RemoveSocket(sockfd);
            } else {
                SocketReady(sockfd);
            }
        }
    ));
}

void EgressHub::SocketReady(int sockfd) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    connections_for_pick_.Add(sockfd);
    if (!write_buffer_.empty()) {
        ScheduleSendFunction();
    }
}

void EgressHub::RemoveSocket(int sockfd) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    connections_for_pick_.Remove(sockfd);
    URING_DCHECK_OK(current_io_uring()->Close(sockfd, [this, sockfd] () {
        int valid_socks = 0;
        for (size_t i = 0; i < sockfds_.size(); i++) {
            if (sockfds_[i] == sockfd) {
                sockfds_[i] = -1;
            } else if (sockfds_[i] >= 0) {
                valid_socks++;
            }
        }
        if (valid_socks == 0) {
            state_ = kClosed;
            io_worker_->OnConnectionClose(this);
        }
    }));
}

void EgressHub::ScheduleSendFunction() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    DCHECK(!write_buffer_.empty());
    if (!send_fn_scheduled_) {
        io_worker_->ScheduleIdleFunction(
            this, absl::bind_front(&EgressHub::SendPendingMessages, this));
        send_fn_scheduled_ = true;
    }
}

void EgressHub::SendPendingMessages() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(ERROR) << "Connection is closing or has closed, will not send this message";
        return;
    }
    DCHECK(send_fn_scheduled_);
    send_fn_scheduled_ = false;
    DCHECK(!write_buffer_.empty());

    int sockfd = -1;
    if (!connections_for_pick_.PickNext(&sockfd)) {
        HLOG(WARNING) << "No ready connections";
        return;
    }
    DCHECK(sockfd >= 0);

    while (!write_buffer_.empty()) {
        std::span<char> buf;
        io_worker_->NewWriteBuffer(&buf);
        size_t copy_size = std::min(buf.size(), write_buffer_.length());
        std::span<const char> data(write_buffer_.data(), copy_size);
        URING_DCHECK_OK(current_io_uring()->SendAll(
            sockfd, CopyToBuffer(buf, data),
            [this, buf, sockfd] (int status) {
                io_worker_->ReturnWriteBuffer(buf);
                if (status != 0) {
                    HPLOG(ERROR) << "Failed to send data";
                    RemoveSocket(sockfd);
                }
            }
        ));
        write_buffer_.ConsumeFront(copy_size);
    }
}

std::string EgressHub::GetLogHeader(int type) {
    int masked_type = type & kConnectionTypeMask;
    switch (masked_type) {
    case kGatewayEgressHubTypeId:
        return "GatewayEgressHub: ";
    case kEngineEgressHubTypeId:
        return fmt::format("EngineEgressHub[{}]: ", type - masked_type);
    case kSequencerEgressHubTypeId:
        return fmt::format("SequencerEgressHub[{}]: ", type - masked_type);
    case kStorageEgressHubTypeId:
        return fmt::format("StorageEgressHub[{}]: ", type - masked_type);
    default:
        return fmt::format("EgressHub[{}]: ", type);
    }
}

}  // namespace server
}  // namespace faas
