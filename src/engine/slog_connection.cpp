#include "engine/slog_connection.h"

#include "utils/socket.h"
#include "engine/flags.h"
#include "engine/constants.h"
#include "engine/slog_engine.h"

namespace faas {
namespace engine {

IncomingSLogConnection::IncomingSLogConnection(SLogEngine* slog_engine, int sockfd)
    : ConnectionBase(kIncomingSLogConnectionTypeId),
      slog_engine_(slog_engine), state_(kCreated), sockfd_(sockfd),
      log_header_("IncomingSLogConnection: ") {}

IncomingSLogConnection::~IncomingSLogConnection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void IncomingSLogConnection::Start(IOWorker* io_worker) {
    DCHECK(state_ == kCreated);
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    if (absl::GetFlag(FLAGS_tcp_enable_keepalive)) {
        CHECK(utils::SetTcpSocketKeepAlive(sockfd_));
    }
    current_io_uring()->PrepareBuffers(kIncomingSLogConnectionBufGroup, kBufSize);
    URING_DCHECK_OK(current_io_uring()->RegisterFd(sockfd_));
    state_ = kRunning;
    URING_DCHECK_OK(current_io_uring()->StartRecv(
        sockfd_, kIncomingSLogConnectionBufGroup,
        absl::bind_front(&IncomingSLogConnection::OnRecvData, this)));
}

void IncomingSLogConnection::ScheduleClose() {
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

bool IncomingSLogConnection::OnRecvData(int status, std::span<const char> data) {
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
                slog_engine_->OnMessageFromOtherEngine(*message);
            });
    }
    return true;
}

SLogMessageHub::SLogMessageHub(SLogEngine* slog_engine)
    : ConnectionBase(kSLogMessageHubTypeId),
      slog_engine_(slog_engine), state_(kCreated),
      log_header_("SLogMessageHub: ") {
}

SLogMessageHub::~SLogMessageHub() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void SLogMessageHub::Start(IOWorker* io_worker) {
    io_worker_ = io_worker;
    state_ = kRunning;
}

class SLogMessageHub::Connection {
public:
    Connection(SLogMessageHub* hub, int conn_id, uint16_t node_id,
               const struct sockaddr_in* addr);
    ~Connection();

    int id() const { return id_; }
    uint16_t node_id() const { return node_id_; }

    void Start(IOWorker* io_worker);
    void ScheduleClose();

    void SendMessage(const protocol::Message& message);

private:
    enum State { kCreated, kConnecting, kRunning, kClosing, kClosed };

    SLogMessageHub* hub_;
    int id_;
    uint16_t node_id_;
    struct sockaddr_in addr_;
    IOWorker* io_worker_;
    State state_;
    int sockfd_;

    std::string log_header_;

    DISALLOW_COPY_AND_ASSIGN(Connection);
};

SLogMessageHub::Connection::Connection(SLogMessageHub* hub, int conn_id,
                                       uint16_t node_id, const struct sockaddr_in* addr)
    : hub_(hub), id_(conn_id), node_id_(node_id),
      state_(kCreated), sockfd_(-1),
      log_header_(fmt::format("OutgoingSLogConnection[{}]: ", node_id)) {
    memcpy(&addr_, addr, sizeof(struct sockaddr_in));
}

SLogMessageHub::Connection::~Connection() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void SLogMessageHub::Connection::Start(IOWorker* io_worker) {
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

void SLogMessageHub::Connection::ScheduleClose() {
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

void SLogMessageHub::Connection::SendMessage(const protocol::Message& message) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(ERROR) << "Connection is closing or has closed, will not send this message";
        return;
    }
    std::span<char> buf;
    io_worker_->NewWriteBuffer(&buf);
    CHECK_GE(buf.size(), sizeof(protocol::Message));
    // TODO: consider payload size, instead of copying the entire message struct
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

struct SLogMessageHub::NodeContext {
    std::vector<protocol::Message> pending_messages;
    absl::flat_hash_set<Connection*> active_connections;
    absl::flat_hash_set<Connection*>::iterator next_connection;

    NodeContext() {
        this->pending_messages.clear();
        this->active_connections.clear();
        this->next_connection = this->active_connections.begin();
    }
};

void SLogMessageHub::SendMessage(uint16_t node_id, const protocol::Message& message) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kRunning) {
        HLOG(WARNING) << "Not in running state, will not send this message";
        return;
    }
    NodeContext* ctx = nullptr;
    if (!node_ctxes_.contains(node_id)) {
        HLOG(INFO) << fmt::format("New node {}", node_id);
        ctx = new NodeContext;
        node_ctxes_[node_id] = std::unique_ptr<NodeContext>(ctx);
        SetupConnections(node_id);
    } else {
        ctx = node_ctxes_[node_id].get();
    }
    // TODO: consider sending messages in batches
    // TODO: consider implementing receiver-side acks to ensure delivery
    if (ctx->active_connections.empty()) {
        HLOG(INFO) << fmt::format("No active connection for node {}", node_id);
        ctx->pending_messages.push_back(message);
        return;
    }
    Connection* conn = *(ctx->next_connection);
    if (++ctx->next_connection == ctx->active_connections.end()) {
        ctx->next_connection = ctx->active_connections.begin();
    }
    conn->SendMessage(message);
}

void SLogMessageHub::SetupConnections(uint16_t node_id) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    std::string_view host;
    uint16_t port;
    CHECK(utils::ParseHostPort(slog_engine_->GetNodeAddr(node_id), &host, &port));
    struct sockaddr_in addr;
    if (!utils::FillTcpSocketAddr(&addr, host, port)) {
        HLOG(FATAL) << fmt::format("Cannot resolve address for node {}", node_id);
    }
    size_t conn_per_worker = absl::GetFlag(FLAGS_shared_log_conn_per_worker);
    for (size_t i = 0; i < conn_per_worker; i++) {
        std::unique_ptr<Connection> conn(
            new Connection(this, next_connection_id_++, node_id, &addr));
        conn->Start(io_worker_);
        connections_[conn->id()] = std::move(conn);
    }
}

void SLogMessageHub::OnConnectionConnected(Connection* conn) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    DCHECK(node_ctxes_.contains(conn->node_id()));
    if (!node_ctxes_.contains(conn->node_id())) {
        conn->ScheduleClose();
        return;
    }
    NodeContext* ctx = node_ctxes_[conn->node_id()].get();
    ctx->active_connections.insert(conn);
    ctx->next_connection = ctx->active_connections.begin();
    while (!ctx->pending_messages.empty()) {
        HLOG(INFO) << "Send pending messages with the new connection";
        conn->SendMessage(ctx->pending_messages.back());
        ctx->pending_messages.pop_back();
    }
}

void SLogMessageHub::OnConnectionClosing(Connection* conn) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    uint16_t node_id = conn->node_id();
    if (node_ctxes_.contains(node_id)) {
        NodeContext* ctx = node_ctxes_[node_id].get();
        DCHECK(ctx->active_connections.contains(conn));
        ctx->active_connections.erase(conn);
        ctx->next_connection = ctx->active_connections.begin();
        if (ctx->active_connections.empty()) {
            node_ctxes_.erase(node_id);
        }
    }
}

void SLogMessageHub::OnConnectionClosed(Connection* conn) {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    DCHECK(connections_.contains(conn->id()));
    connections_.erase(conn->id());
    if (state_ == kClosing && connections_.empty()) {
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }
}

void SLogMessageHub::ScheduleClose() {
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
        for (const auto& entry : connections_) {
            Connection* conn = entry.second.get();
            conn->ScheduleClose();
        }
        state_ = kClosing;
    }    
}

}  // namespace engine
}  // namespace faas
