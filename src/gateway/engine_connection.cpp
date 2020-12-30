#include "gateway/engine_connection.h"

#include "utils/socket.h"
#include "gateway/flags.h"
#include "gateway/server.h"

namespace faas {
namespace gateway {

using protocol::GatewayMessage;

EngineConnection::EngineConnection(Server* server, uint16_t node_id, uint16_t conn_id,
                                   int sockfd)
    : server::ConnectionBase(type_id(node_id)),
      server_(server),
      node_id_(node_id),
      conn_id_(conn_id),
      state_(kCreated),
      sockfd_(sockfd),
      log_header_(fmt::format("EngineConnection[{}-{}]: ", node_id, conn_id)) {}

EngineConnection::~EngineConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void EngineConnection::Start(server::IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    if (absl::GetFlag(FLAGS_tcp_enable_nodelay)) {
        CHECK(utils::SetTcpSocketNoDelay(sockfd_));
    }
    if (absl::GetFlag(FLAGS_tcp_enable_keepalive)) {
        CHECK(utils::SetTcpSocketKeepAlive(sockfd_));
    }
    current_io_uring()->PrepareBuffers(kEngineConnectionBufGroup, kBufSize);
    URING_DCHECK_OK(current_io_uring()->RegisterFd(sockfd_));
    URING_DCHECK_OK(current_io_uring()->StartRecv(
        sockfd_, kEngineConnectionBufGroup,
        absl::bind_front(&EngineConnection::OnRecvData, this)));
    state_ = kRunning;
    server_->node_manager()->OnNewEngineConnection(this);
    ProcessGatewayMessages();
}

void EngineConnection::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    URING_DCHECK_OK(current_io_uring()->Close(sockfd_, [this] () {
        DCHECK(state_ == kClosing);
        state_ = kClosed;
        server_->node_manager()->OnEngineConnectionClosed(this);
        io_worker_->OnConnectionClose(this);
    }));
    state_ = kClosing;
}

void EngineConnection::SendMessage(const GatewayMessage& message, std::span<const char> payload) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(WARNING) << "EngineConnection is closing or has closed, will not send this message";
        return;
    }
    DCHECK_EQ(message.payload_size, gsl::narrow_cast<int32_t>(payload.size()));
    size_t pos = 0;
    while (pos < sizeof(GatewayMessage) + payload.size()) {
        std::span<char> buf;
        io_worker_->NewWriteBuffer(&buf);
        size_t write_size;
        if (pos == 0) {
            DCHECK_LE(sizeof(GatewayMessage), buf.size());
            memcpy(buf.data(), &message, sizeof(GatewayMessage));
            size_t copy_size = std::min(buf.size() - sizeof(GatewayMessage), payload.size());
            memcpy(buf.data() + sizeof(GatewayMessage), payload.data(), copy_size);
            write_size = sizeof(GatewayMessage) + copy_size;
        } else {
            size_t copy_size = std::min(buf.size(), payload.size() + sizeof(GatewayMessage) - pos);
            memcpy(buf.data(), payload.data() + pos - sizeof(GatewayMessage), copy_size);
            write_size = copy_size;
        }
        DCHECK_LE(write_size, buf.size());
        URING_DCHECK_OK(current_io_uring()->SendAll(
            sockfd_, std::span<const char>(buf.data(), write_size),
            [this, buf] (int status) {
                io_worker_->ReturnWriteBuffer(buf);
                if (status != 0) {
                    HPLOG(ERROR) << "Failed to send data, will close this connection";
                    ScheduleClose();
                }
            }
        ));
        pos += write_size;
    }
}

void EngineConnection::ProcessGatewayMessages() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    while (read_buffer_.length() >= sizeof(GatewayMessage)) {
        GatewayMessage* message = reinterpret_cast<GatewayMessage*>(read_buffer_.data());
        size_t full_size = sizeof(GatewayMessage) + std::max<size_t>(0, message->payload_size);
        if (read_buffer_.length() >= full_size) {
            std::span<const char> payload(read_buffer_.data() + sizeof(GatewayMessage),
                                          full_size - sizeof(GatewayMessage));
            server_->OnRecvEngineMessage(this, *message, payload);
            read_buffer_.ConsumeFront(full_size);
        } else {
            break;
        }
    }
}

bool EngineConnection::OnRecvData(int status, std::span<const char> data) {
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
        ProcessGatewayMessages();
        return true;
    }
}

}  // namespace gateway
}  // namespace faas
