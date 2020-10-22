#include "engine/gateway_connection.h"

#include "utils/socket.h"
#include "engine/engine.h"

#define HLOG(l) LOG(l) << log_header_
#define HPLOG(l) PLOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

#include <absl/flags/flag.h>

ABSL_FLAG(bool, gateway_conn_enable_nodelay, true,
          "Enable TCP_NODELAY for connections to gateway");
ABSL_FLAG(bool, gateway_conn_enable_keepalive, true,
          "Enable TCP keep-alive for connections to gateway");

namespace faas {
namespace engine {

using protocol::GatewayMessage;
using protocol::NewEngineHandshakeGatewayMessage;

GatewayConnection::GatewayConnection(Engine* engine, uint16_t conn_id, int sockfd)
    : ConnectionBase(kTypeId),
      engine_(engine), conn_id_(conn_id), state_(kCreated), sockfd_(sockfd),
      log_header_(fmt::format("GatewayConnection[{}]: ", conn_id)) {}

GatewayConnection::~GatewayConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void GatewayConnection::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    if (absl::GetFlag(FLAGS_gateway_conn_enable_nodelay)) {
        CHECK(utils::SetTcpSocketNoDelay(sockfd_));
    }
    if (absl::GetFlag(FLAGS_gateway_conn_enable_keepalive)) {
        CHECK(utils::SetTcpSocketKeepAlive(sockfd_));
    }
    current_io_uring()->PrepareBuffers(kBufGroup, kBufSize);
    handshake_message_ = NewEngineHandshakeGatewayMessage(engine_->node_id(), conn_id_);
    URING_DCHECK_OK(current_io_uring()->SendAll(
        sockfd_, std::span<const char>(reinterpret_cast<const char*>(&handshake_message_),
                                       sizeof(GatewayMessage)),
        [this] (int status) {
            if (status != 0) {
                PLOG(ERROR) << "Failed to send handshake, will close this connection";
            } else {
                HLOG(INFO) << "Handshake done";
                state_ = kRunning;
                URING_DCHECK_OK(current_io_uring()->StartRecv(
                    sockfd_, kBufGroup,
                    absl::bind_front(&GatewayConnection::OnRecvData, this)));
            }
        }
    ));
    state_ = kHandshake;
}

void GatewayConnection::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kHandshake || state_ == kRunning);
    current_io_uring()->StopReadOrRecv(sockfd_);
    URING_DCHECK_OK(current_io_uring()->Close(sockfd_, [this] () {
        DCHECK(state_ == kClosing);
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }));
    state_ = kClosing;
}

void GatewayConnection::SendMessage(const GatewayMessage& message, std::span<const char> payload) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(WARNING) << "GatewayConnection is closing or has closed, will not send this message";
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

void GatewayConnection::ProcessGatewayMessages() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    while (read_buffer_.length() >= sizeof(GatewayMessage)) {
        GatewayMessage* message = reinterpret_cast<GatewayMessage*>(read_buffer_.data());
        size_t full_size = sizeof(GatewayMessage) + std::max<size_t>(0, message->payload_size);
        if (read_buffer_.length() >= full_size) {
            std::span<const char> payload(read_buffer_.data() + sizeof(GatewayMessage),
                                          full_size - sizeof(GatewayMessage));
            engine_->OnRecvGatewayMessage(this, *message, payload);
            read_buffer_.ConsumeFront(full_size);
        } else {
            break;
        }
    }
}

bool GatewayConnection::OnRecvData(int status, std::span<const char> data) {
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

}  // namespace engine
}  // namespace faas
