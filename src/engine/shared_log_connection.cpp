#include "engine/shared_log_connection.h"

#include "common/flags.h"
#include "utils/socket.h"
#include "engine/engine.h"
#include "engine/shared_log_engine.h"

#define HLOG(l) LOG(l) << log_header_
#define HPLOG(l) PLOG(l) << log_header_
#define HVLOG(l) VLOG(l) << log_header_

namespace faas {
namespace engine {

IncomingSharedLogConnection::IncomingSharedLogConnection(Engine* engine, int sockfd)
    : ConnectionBase(kTypeId),
      engine_(engine), state_(kCreated), sockfd_(sockfd),
      log_header_("IncomingSharedLogConnection: ") {}

IncomingSharedLogConnection::~IncomingSharedLogConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void IncomingSharedLogConnection::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    if (absl::GetFlag(FLAGS_tcp_enable_keepalive)) {
        CHECK(utils::SetTcpSocketKeepAlive(sockfd_));
    }
    current_io_uring()->PrepareBuffers(kBufGroup, kBufSize);
    URING_DCHECK_OK(current_io_uring()->RegisterFd(sockfd_));
    state_ = kRunning;
    URING_DCHECK_OK(current_io_uring()->StartRecv(
        sockfd_, kBufGroup,
        absl::bind_front(&IncomingSharedLogConnection::OnRecvData, this)));
}

void IncomingSharedLogConnection::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    current_io_uring()->StopReadOrRecv(sockfd_);
    URING_DCHECK_OK(current_io_uring()->Close(sockfd_, [this] () {
        DCHECK(state_ == kClosing);
        URING_DCHECK_OK(current_io_uring()->UnregisterFd(sockfd_));
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }));
    state_ = kClosing;
}

bool IncomingSharedLogConnection::OnRecvData(int status, std::span<const char> data) {
    if (status != 0) {
        HPLOG(ERROR) << "Read error, will close this connection";
        ScheduleClose();
        return false;
    } else if (data.size() == 0) {
        HLOG(INFO) << "Connection closed remotely";
        ScheduleClose();
        return false;
    } else {
        utils::ReadMessages<protocol::Message>(
            &message_buffer_, data.data(), data.size(),
            [this] (protocol::Message* message) {
                engine_->OnRecvSharedLogMessage(*message);
            });
    }
    return true;
}

SharedLogMessageHub::SharedLogMessageHub(Engine* engine, SharedLogEngine* shared_log_engine)
    : ConnectionBase(kTypeId),
      engine_(engine), shared_log_engine_(shared_log_engine), state_(kCreated),
      log_header_("SharedLogMessageHub: ") {
}

SharedLogMessageHub::~SharedLogMessageHub() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void SharedLogMessageHub::Start(IOWorker* io_worker) {
    io_worker_ = io_worker;
    state_ = kRunning;
}

class SharedLogMessageHub::Connection {
public:
    Connection(SharedLogMessageHub* hub,
               uint16_t view_id, uint16_t node_id,
               const struct sockaddr_in* addr);
    ~Connection();

    uint16_t view_id() const { return view_id_; }
    uint16_t node_id() const { return node_id_; }

    void Start(IOWorker* io_worker);
    void ScheduleClose();

    void SendMessage(const protocol::Message& message);

private:
    enum State { kCreated, kConnecting, kRunning, kClosing, kClosed };

    SharedLogMessageHub* hub_;
    uint16_t view_id_;
    uint16_t node_id_;
    struct sockaddr_in addr_;
    IOWorker* io_worker_;
    State state_;
    int sockfd_;

    std::string log_header_;

    DISALLOW_COPY_AND_ASSIGN(Connection);
};

SharedLogMessageHub::Connection::Connection(SharedLogMessageHub* hub,
                                            uint16_t view_id, uint16_t node_id,
                                            const struct sockaddr_in* addr)
    : hub_(hub), view_id_(view_id), node_id_(node_id),
      state_(kCreated), sockfd_(-1),
      log_header_("OutgoingEngineConnection: ") {
    memcpy(&addr_, addr, sizeof(struct sockaddr_in));
}

SharedLogMessageHub::Connection::~Connection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void SharedLogMessageHub::Connection::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    sockfd_ = socket(AF_INET, SOCK_STREAM, 0);
    PCHECK(sockfd_ >= 0) << "Failed to create socket";
    state_ = kConnecting;
    URING_DCHECK_OK(current_io_uring()->RegisterFd(sockfd_));
    URING_DCHECK_OK(current_io_uring()->Connect(
        sockfd_, reinterpret_cast<struct sockaddr*>(&addr_), sizeof(addr_),
        [this] (int status) {
            if (status != 0) {
                HPLOG(ERROR) << "Failed to connect";
                ScheduleClose();
                return;
            }
            state_ = kRunning;
            if (absl::GetFlag(FLAGS_tcp_enable_nodelay)) {
                CHECK(utils::SetTcpSocketNoDelay(sockfd_));
            }
            if (absl::GetFlag(FLAGS_tcp_enable_keepalive)) {
                CHECK(utils::SetTcpSocketKeepAlive(sockfd_));
            }
            hub_->OnConnectionConnected(this);
        }
    ));
}

void SharedLogMessageHub::Connection::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kConnecting || state_ == kRunning);
    URING_DCHECK_OK(current_io_uring()->Close(sockfd_, [this] () {
        DCHECK(state_ == kClosing);
        URING_DCHECK_OK(current_io_uring()->UnregisterFd(sockfd_));
        state_ = kClosed;
        hub_->OnConnectionClosed(this);
    }));
    state_ = kClosing;
    hub_->OnConnectionClosing(this);
}

void SharedLogMessageHub::Connection::SendMessage(const protocol::Message& message) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(ERROR) << "Connection is closing or has closed, will not send this message";
        return;
    }
    std::span<char> buf;
    io_worker_->NewWriteBuffer(&buf);
    CHECK_GE(buf.size(), sizeof(protocol::Message));
    memcpy(buf.data(), &message, sizeof(protocol::Message));
    URING_DCHECK_OK(current_io_uring()->SendAll(
        sockfd_, std::span<const char>(buf.data(), sizeof(protocol::Message)),
        [this, buf] (int status) {
            io_worker_->ReturnWriteBuffer(buf);
            if (status != 0) {
                HPLOG(ERROR) << "Failed to write response, will close this connection";
                ScheduleClose();
            }
        }
    ));
}

struct SharedLogMessageHub::NodeContext {
    uint16_t view_id;
    std::vector<protocol::Message> pending_messages;
    absl::flat_hash_set<Connection*> active_connections;
    absl::flat_hash_set<Connection*>::iterator next_connection;

    void reset(uint16_t view_id) {
        this->view_id = view_id;
        this->pending_messages.clear();
        for (Connection* conn : this->active_connections) {
            conn->ScheduleClose();
        }
        this->active_connections.clear();
        this->next_connection = this->active_connections.begin();
    }
};

void SharedLogMessageHub::SendMessage(uint16_t view_id, uint16_t node_id,
                                      const protocol::Message& message) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(WARNING) << "Not in running state, will not send this message";
        return;
    }
    NodeContext* ctx = nullptr;
    if (!node_ctxes_.contains(node_id)) {
        ctx = new NodeContext;
        ctx->reset(view_id);
        node_ctxes_[node_id] = std::unique_ptr<NodeContext>(ctx);
        SetupConnections(view_id, node_id);
    } else {
        ctx = node_ctxes_[node_id].get();
        if (view_id < ctx->view_id) {
            HLOG(WARNING) << "Outdated message";
            return;
        } else if (view_id > ctx->view_id) {
            for (Connection* conn : ctx->active_connections) {
                conn->ScheduleClose();
            }
            ctx->reset(view_id);
            SetupConnections(view_id, node_id);
        }
    }
    // TODO: consider sending messages in batches
    // TODO: consider implementing receiver-side acks to ensure delivery
    if (ctx->active_connections.empty()) {
        ctx->pending_messages.push_back(message);
        return;
    }
    Connection* conn = *(ctx->next_connection);
    if (++ctx->next_connection == ctx->active_connections.end()) {
        ctx->next_connection = ctx->active_connections.begin();
    }
    DCHECK_EQ(conn->view_id(), view_id);
    conn->SendMessage(message);
}

void SharedLogMessageHub::SetupConnections(uint16_t view_id, uint16_t node_id) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    std::string_view host;
    uint16_t port;
    CHECK(utils::ParseHostPort(shared_log_engine_->GetNodeAddr(view_id, node_id), &host, &port));
    struct sockaddr_in addr;
    if (!utils::FillTcpSocketAddr(&addr, host, port)) {
        HLOG(FATAL) << fmt::format("Cannot resolve address for node {}", node_id);
    }
    for (int i = 0; i < engine_->engine_conn_per_worker(); i++) {
        std::unique_ptr<Connection> conn(new Connection(this, view_id, node_id, &addr));
        conn->Start(io_worker_);
        connections_.insert(std::move(conn));
    }
}

void SharedLogMessageHub::OnConnectionConnected(Connection* conn) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    DCHECK(node_ctxes_.contains(conn->node_id()));
    if (!node_ctxes_.contains(conn->node_id())) {
        conn->ScheduleClose();
        return;
    }
    NodeContext* ctx = node_ctxes_[conn->node_id()].get();
    DCHECK_GE(ctx->view_id, conn->view_id());
    if (ctx->view_id == conn->view_id()) {
        ctx->active_connections.insert(conn);
        ctx->next_connection = ctx->active_connections.begin();
        while (!ctx->pending_messages.empty()) {
            conn->SendMessage(ctx->pending_messages.back());
            ctx->pending_messages.pop_back();
        }
    } else {
        conn->ScheduleClose();
    }
}

void SharedLogMessageHub::OnConnectionClosing(Connection* conn) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (node_ctxes_.contains(conn->node_id())) {
        NodeContext* ctx = node_ctxes_[conn->node_id()].get();
        if (ctx->view_id == conn->view_id()) {
            DCHECK(ctx->active_connections.contains(conn));
            ctx->active_connections.erase(conn);
            ctx->next_connection = ctx->active_connections.begin();
        }
    }
}

void SharedLogMessageHub::OnConnectionClosed(Connection* conn) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    DCHECK(connections_.contains(conn));
    connections_.erase(conn);
    if (state_ == kClosing && connections_.empty()) {
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }
}

void SharedLogMessageHub::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    if (connections_.empty()) {
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    } else {
        for (const auto& conn : connections_) {
            conn->ScheduleClose();
        }
        state_ = kClosing;
    }    
}

}  // namespace engine
}  // namespace faas
