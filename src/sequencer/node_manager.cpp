#include "sequencer/node_manager.h"

#include "common/flags.h"
#include "utils/appendable_buffer.h"
#include "sequencer/server.h"

#define HLOG(l) LOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace sequencer {

using protocol::SequencerMessage;
using protocol::SequencerMessageHelper;

NodeManager::NodeManager(Server* server)
    : server_(server), state_(kCreated),
      log_header_("NodeManager: "),
      uv_loop_(nullptr),
      buffer_pool_("NodeManager[ReadWrite]", kBufferSize) {}

NodeManager::~NodeManager() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void NodeManager::Start(uv_loop_t* uv_loop, std::string_view listen_addr, uint16_t listen_port) {
    uv_loop_ = uv_loop;
    UV_CHECK_OK(uv_tcp_init(uv_loop, &uv_handle_));
    struct sockaddr_in bind_addr;
    std::string addr(listen_addr);
    UV_CHECK_OK(uv_ip4_addr(addr.c_str(), listen_port, &bind_addr));
    UV_CHECK_OK(uv_tcp_bind(&uv_handle_, (const struct sockaddr *)&bind_addr, 0));
    HLOG(INFO) << fmt::format("Listen on {}:{} for engine connections", listen_addr, listen_port);
    UV_CHECK_OK(uv_listen(UV_AS_STREAM(&uv_handle_), kListenBackLog,
                          &NodeManager::EngineConnectionCallback));
    state_ = kRunning;
}

class NodeManager::Connection : public uv::Base {
public:
    explicit Connection(NodeManager* node_manager);
    ~Connection();

    uint16_t node_id() const { return node_id_; }
    std::string_view shared_log_addr() const { return shared_log_addr_; }

    uv_tcp_t* uv_handle() { return &uv_handle_; }    

    void Start();
    void ScheduleClose();

    void SendMessage(const SequencerMessage& message, std::span<const char> payload);

private:
    enum State { kCreated, kHandshaking, kRunning, kClosing, kClosed };

    NodeManager* node_manager_;
    State state_;
    uint16_t node_id_;
    std::string shared_log_addr_;
    std::string log_header_;

    uv_tcp_t uv_handle_;
    utils::AppendableBuffer read_buffer_;

    void ProcessMessages();
    void OnMessage(const SequencerMessage& message, std::span<const char> payload);

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(RecvData);
    DECLARE_UV_WRITE_CB_FOR_CLASS(DataSent);
    DECLARE_UV_CLOSE_CB_FOR_CLASS(Close);

    DISALLOW_COPY_AND_ASSIGN(Connection);
};

NodeManager::Connection::Connection(NodeManager* node_manager)
    : node_manager_(node_manager),
      state_(kCreated),
      log_header_("EngineConnection[Handshking]: ") {
    UV_CHECK_OK(uv_tcp_init(node_manager_->uv_loop_, &uv_handle_));
    uv_handle_.data = this;
}

void NodeManager::Connection::Start() {
    if (absl::GetFlag(FLAGS_tcp_enable_nodelay)) {
        UV_DCHECK_OK(uv_tcp_nodelay(&uv_handle_, 1));
    }
    if (absl::GetFlag(FLAGS_tcp_enable_keepalive)) {
        UV_DCHECK_OK(uv_tcp_keepalive(&uv_handle_, 1, 1));
    }
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(&uv_handle_),
                               &NodeManager::Connection::BufferAllocCallback,
                               &NodeManager::Connection::RecvDataCallback));
    state_ = kHandshaking;
}

void NodeManager::Connection::ScheduleClose() {
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kHandshaking || state_ == kRunning);
    uv_close(UV_AS_HANDLE(&uv_handle_), &NodeManager::Connection::CloseCallback);
    state_ = kClosing;
    node_manager_->OnConnectionClosing(this);
}

void NodeManager::Connection::SendMessage(const SequencerMessage& message,
                                          std::span<const char> payload) {
    // TODO
}

void NodeManager::Connection::ProcessMessages() {
    while (read_buffer_.length() >= sizeof(SequencerMessage)) {
        SequencerMessage* message = reinterpret_cast<SequencerMessage*>(read_buffer_.data());
        size_t full_size = sizeof(SequencerMessage) + std::max<size_t>(0, message->payload_size);
        if (read_buffer_.length() >= full_size) {
            std::span<const char> payload(read_buffer_.data() + sizeof(SequencerMessage),
                                          full_size - sizeof(SequencerMessage));
            OnMessage(*message, payload);
            read_buffer_.ConsumeFront(full_size);
        } else {
            break;
        }
    }
}

void NodeManager::Connection::OnMessage(const SequencerMessage& message,
                                        std::span<const char> payload) {
    if (state_ == kHandshaking) {
        if (!SequencerMessageHelper::IsEngineHandshake(message)) {
            HLOG(ERROR) << "The first message is not handshake";
            ScheduleClose();
            return;
        }
        node_id_ = message.node_id;
        shared_log_addr_ = std::string(message.shared_log_addr, strlen(message.shared_log_addr));
        state_ = kRunning;
        node_manager_->OnConnectionHandshaked(this);
    } else {
        node_manager_->server_->OnRecvNodeMessage(node_id_, message, payload);
    }
}

UV_ALLOC_CB_FOR_CLASS(NodeManager::Connection, BufferAlloc) {
    node_manager_->buffer_pool_.Get(&buf->base, &buf->len);
}

UV_READ_CB_FOR_CLASS(NodeManager::Connection, RecvData) {
    auto reclaim_resource = gsl::finally([this, buf] {
        if (buf->base != 0) {
            node_manager_->buffer_pool_.Return(buf->base);
        }
    });
    if (nread < 0) {
        if (nread == UV_EOF) {
            HLOG(INFO) << "Connection closed remotely";
        } else {
            HLOG(ERROR) << "Read error, will close this connection: "
                        << uv_strerror(nread);
        }
        ScheduleClose();
        return;
    }
    if (nread == 0) {
        return;
    }
    read_buffer_.AppendData(buf->base, nread);
    ProcessMessages();
}

UV_WRITE_CB_FOR_CLASS(NodeManager::Connection, DataSent) {
    auto reclaim_resource = gsl::finally([this, req] {
        node_manager_->buffer_pool_.Return(reinterpret_cast<char*>(req->data));
        node_manager_->write_req_pool_.Return(req);
    });
    if (status != 0) {
        HLOG(ERROR) << "Failed to send data, will close this connection: "
                    << uv_strerror(status);
        ScheduleClose();
    }
}

UV_CLOSE_CB_FOR_CLASS(NodeManager::Connection, Close) {
    DCHECK(state_ == kClosing);
    state_ = kClosed;
    node_manager_->OnConnectionClosed(this);
}

void NodeManager::ScheduleClose() {
    // TODO
}

bool NodeManager::SendMessage(uint16_t node_id, const SequencerMessage& message,
                              std::span<const char> payload) {
    // TODO

    return true;
}

void NodeManager::OnConnectionHandshaked(Connection* connection) {
    uint16_t node_id = connection->node_id();
    bool new_node = false;
    NodeContext* node_ctx = nullptr;
    if (!connected_nodes_.contains(node_id)) {
        node_ctx = new NodeContext;
        node_ctx->node_id = node_id;
        connected_nodes_[node_id] = std::unique_ptr<NodeContext>(node_ctx);
        new_node = true;
    } else {
        node_ctx = connected_nodes_[node_id].get();
    }
    node_ctx->active_connections.insert(connection);
    node_ctx->next_connection = node_ctx->active_connections.begin();
    if (new_node) {
        server_->OnNewNodeConnected(node_id, connection->shared_log_addr());
    }
}

void NodeManager::OnConnectionClosing(Connection* connection) {
    uint16_t node_id = connection->node_id();
    if (!connected_nodes_.contains(node_id)) {
        HLOG(ERROR) << fmt::format("connected_nodes_ does not contain node_id {}", node_id);
        return;
    }
    NodeContext* node_ctx = connected_nodes_[node_id].get();
    DCHECK(node_ctx->active_connections.contains(connection));
    node_ctx->active_connections.erase(connection);
    node_ctx->next_connection = node_ctx->active_connections.begin();
    if (node_ctx->active_connections.empty()) {
        connected_nodes_.erase(node_id);
        server_->OnNodeDisconnected(node_id);
    }
}

void NodeManager::OnConnectionClosed(Connection* connection) {
    connections_.erase(connection);
}

UV_CONNECTION_CB_FOR_CLASS(NodeManager, EngineConnection) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to open engine connection: " << uv_strerror(status);
        return;
    }
    std::unique_ptr<Connection> conn(new Connection(this));
    if (uv_accept(UV_AS_STREAM(&uv_handle_), UV_AS_STREAM(conn->uv_handle())) == 0) {
        conn->Start();
        connections_.insert(std::move(conn));
    } else {
        HLOG(ERROR) << "Failed to accept new engine connection";
    }
}

}  // namespace sequencer
}  // namespace faas
