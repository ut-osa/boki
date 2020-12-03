#include "gateway/server.h"

#include "ipc/base.h"
#include "ipc/shm_region.h"
#include "common/time.h"
#include "utils/fs.h"
#include "utils/io.h"
#include "utils/docker.h"
#include "worker/worker_lib.h"
#include "gateway/flags.h"

#define log_header_ "Server: "

namespace faas {
namespace gateway {

using protocol::FuncCall;
using protocol::FuncCallHelper;
using protocol::GatewayMessage;
using protocol::GatewayMessageHelper;

Server::Server()
    : engine_conn_port_(-1),
      http_port_(-1),
      grpc_port_(-1),
      num_io_workers_(kDefaultNumIOWorkers),
      next_http_conn_worker_id_(0),
      next_grpc_conn_worker_id_(0),
      next_http_connection_id_(0),
      next_grpc_connection_id_(0),
      node_manager_(this),
      read_buffer_pool_("HandshakeRead", 128),
      next_call_id_(1),
      last_request_timestamp_(-1),
      incoming_requests_stat_(
          stat::Counter::StandardReportCallback("incoming_requests")),
      request_interval_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("request_interval")),
      requests_instant_rps_stat_(
          stat::StatisticsCollector<float>::StandardReportCallback("requests_instant_rps")),
      inflight_requests_stat_(
          stat::StatisticsCollector<uint16_t>::StandardReportCallback("inflight_requests")),
      running_requests_stat_(
          stat::StatisticsCollector<uint16_t>::StandardReportCallback("running_requests")),
      queueing_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("queueing_delay")),
      dispatch_overhead_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("dispatch_overhead")) {
    UV_CHECK_OK(uv_tcp_init(uv_loop(), &uv_engine_conn_handle_));
    uv_engine_conn_handle_.data = this;
    UV_CHECK_OK(uv_tcp_init(uv_loop(), &uv_http_handle_));
    uv_http_handle_.data = this;
    UV_CHECK_OK(uv_tcp_init(uv_loop(), &uv_grpc_handle_));
    uv_grpc_handle_.data = this;
}

Server::~Server() {}

void Server::StartInternal() {
    // Load function config file
    CHECK(!func_config_file_.empty());
    CHECK(fs_utils::ReadContents(func_config_file_, &func_config_json_))
        << "Failed to read from file " << func_config_file_;
    CHECK(func_config_.Load(func_config_json_));
    // Start IO workers
    CHECK_GT(num_io_workers_, 0);
    HLOG(INFO) << fmt::format("Start {} IO workers", num_io_workers_);
    for (int i = 0; i < num_io_workers_; i++) {
        auto io_worker = CreateIOWorker(fmt::format("IO-{}", i));
        io_workers_.push_back(io_worker);
    }
    struct sockaddr_in bind_addr;
    CHECK(!address_.empty());
    CHECK_NE(engine_conn_port_, -1);
    CHECK_NE(http_port_, -1);
    // Listen on address:engine_conn_port for engine connections
    int listen_backlog = absl::GetFlag(FLAGS_socket_listen_backlog);
    UV_CHECK_OK(uv_ip4_addr(address_.c_str(), engine_conn_port_, &bind_addr));
    UV_CHECK_OK(uv_tcp_bind(&uv_engine_conn_handle_, (const struct sockaddr *)&bind_addr, 0));
    HLOG(INFO) << fmt::format("Listen on {}:{} for engine connections",
                              address_, engine_conn_port_);
    UV_CHECK_OK(uv_listen(
        UV_AS_STREAM(&uv_engine_conn_handle_), listen_backlog,
        &Server::EngineConnectionCallback));
    // Listen on address:http_port for HTTP requests
    UV_CHECK_OK(uv_ip4_addr(address_.c_str(), http_port_, &bind_addr));
    UV_CHECK_OK(uv_tcp_bind(&uv_http_handle_, (const struct sockaddr *)&bind_addr, 0));
    HLOG(INFO) << fmt::format("Listen on {}:{} for HTTP requests", address_, http_port_);
    UV_CHECK_OK(uv_listen(
        UV_AS_STREAM(&uv_http_handle_), listen_backlog,
        &Server::HttpConnectionCallback));
    // Listen on address:grpc_port for HTTP requests
    UV_CHECK_OK(uv_ip4_addr(address_.c_str(), grpc_port_, &bind_addr));
    UV_CHECK_OK(uv_tcp_bind(&uv_grpc_handle_, (const struct sockaddr *)&bind_addr, 0));
    HLOG(INFO) << fmt::format("Listen on {}:{} for gRPC requests", address_, grpc_port_);
    UV_CHECK_OK(uv_listen(
        UV_AS_STREAM(&uv_grpc_handle_), listen_backlog,
        &Server::GrpcConnectionCallback));
}

void Server::StopInternal() {
    uv_close(UV_AS_HANDLE(&uv_engine_conn_handle_), nullptr);
    uv_close(UV_AS_HANDLE(&uv_http_handle_), nullptr);
    uv_close(UV_AS_HANDLE(&uv_grpc_handle_), nullptr);
}

void Server::OnConnectionClose(server::ConnectionBase* connection) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop());
    if (connection->type() == HttpConnection::kTypeId
          || connection->type() == GrpcConnection::kTypeId) {
        absl::MutexLock lk(&mu_);
        DCHECK(connections_.contains(connection->id()));
        connections_.erase(connection->id());
    } else if (connection->type() >= EngineConnection::kBaseTypeId) {
        EngineConnection* engine_connection = connection->as_ptr<EngineConnection>();
        HLOG(WARNING) << fmt::format("EngineConnection (node_id={}, conn_id={}) disconnected",
                                     engine_connection->node_id(), engine_connection->conn_id());
        absl::MutexLock lk(&mu_);
        DCHECK(engine_connections_.contains(connection->id()));
        engine_connections_.erase(connection->id());
    } else {
        UNREACHABLE();
    }
}

void Server::OnNewHttpFuncCall(HttpConnection* connection, FuncCallContext* func_call_context) {
    auto func_entry = func_config_.find_by_func_name(func_call_context->func_name());
    if (func_entry == nullptr) {
        func_call_context->set_status(FuncCallContext::kNotFound);
        connection->OnFuncCallFinished(func_call_context);
        return;
    }
    FuncCall func_call = FuncCallHelper::New(gsl::narrow_cast<uint16_t>(func_entry->func_id),
                                             /* client_id= */ 0, next_call_id_.fetch_add(1));
    VLOG(1) << "OnNewHttpFuncCall: " << FuncCallHelper::DebugString(func_call);
    func_call_context->set_func_call(func_call);
    OnNewFuncCallCommon(connection->ref_self(), func_call_context);
}

void Server::OnNewGrpcFuncCall(GrpcConnection* connection, FuncCallContext* func_call_context) {
    auto func_entry = func_config_.find_by_func_name(func_call_context->func_name());
    std::string method_name(func_call_context->method_name());
    if (func_entry == nullptr || !func_entry->is_grpc_service
          || func_entry->grpc_method_ids.count(method_name) == 0) {
        func_call_context->set_status(FuncCallContext::kNotFound);
        connection->OnFuncCallFinished(func_call_context);
        return;
    }
    FuncCall func_call = FuncCallHelper::NewWithMethod(
        gsl::narrow_cast<uint16_t>(func_entry->func_id),
        gsl::narrow_cast<uint16_t>(func_entry->grpc_method_ids.at(method_name)),
        /* client_id= */ 0, next_call_id_.fetch_add(1));
    VLOG(1) << "OnNewGrpcFuncCall: " << FuncCallHelper::DebugString(func_call);
    func_call_context->set_func_call(func_call);
    OnNewFuncCallCommon(connection->ref_self(), func_call_context);
}

void Server::DiscardFuncCall(FuncCallContext* func_call_context) {
    absl::MutexLock lk(&mu_);
    discarded_func_calls_.insert(func_call_context->func_call().full_call_id);
}

void Server::OnNewConnectedNode(EngineConnection* connection) {
    TryDispatchingPendingFuncCalls();
}

void Server::TryDispatchingPendingFuncCalls() {
    mu_.Lock();
    while (!pending_func_calls_.empty()) {
        FuncCallState state = std::move(pending_func_calls_.front());
        pending_func_calls_.pop_front();
        FuncCall func_call = state.func_call;
        if (discarded_func_calls_.contains(func_call.full_call_id)) {
            discarded_func_calls_.erase(func_call.full_call_id);
            continue;
        }
        std::shared_ptr<server::ConnectionBase> parent_connection;
        if (!connections_.contains(state.connection_id)) {
            continue;
        }
        parent_connection = connections_[state.connection_id];
        mu_.Unlock();
        uint16_t node_id;
        bool node_picked = node_manager_.PickNodeForNewFuncCall(func_call, &node_id);
        bool dispatch_success = false;
        if (node_picked) {
            dispatch_success = DispatchFuncCall(std::move(parent_connection),
                                                state.context, node_id);
        }
        mu_.Lock();
        if (!node_picked) {
            pending_func_calls_.push_front(std::move(state));
            break;
        }
        state.dispatch_timestamp = GetMonotonicMicroTimestamp();
        queueing_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(
            state.dispatch_timestamp - state.recv_timestamp));
        if (dispatch_success) {
            running_func_calls_[func_call.full_call_id] = std::move(state);
            running_requests_stat_.AddSample(
                gsl::narrow_cast<uint16_t>(running_func_calls_.size()));
        }
    }
    mu_.Unlock();
}

void Server::HandleFuncCallCompleteOrFailedMessage(uint16_t node_id,
                                                   const protocol::GatewayMessage& message,
                                                   std::span<const char> payload) {
    DCHECK(GatewayMessageHelper::IsFuncCallComplete(message)
             || GatewayMessageHelper::IsFuncCallFailed(message));
    FuncCall func_call = GatewayMessageHelper::GetFuncCall(message);
    node_manager_.FuncCallFinished(func_call, node_id);
    FuncCallContext* func_call_context = nullptr;
    std::shared_ptr<server::ConnectionBase> parent_connection;
    {
        absl::MutexLock lk(&mu_);
        if (running_func_calls_.contains(func_call.full_call_id)) {
            const FuncCallState& full_call_state = running_func_calls_[func_call.full_call_id];
            // Check if corresponding connection is still active
            if (connections_.contains(full_call_state.connection_id)
                    && !discarded_func_calls_.contains(func_call.full_call_id)) {
                parent_connection = connections_[full_call_state.connection_id];
                func_call_context = full_call_state.context;
            }
            if (discarded_func_calls_.contains(func_call.full_call_id)) {
                discarded_func_calls_.erase(func_call.full_call_id);
            }
            dispatch_overhead_stat_.AddSample(gsl::narrow_cast<int32_t>(
                GetMonotonicMicroTimestamp()
                - full_call_state.dispatch_timestamp
                - message.processing_time));
            running_func_calls_.erase(func_call.full_call_id);
        }
    }
    if (func_call_context != nullptr) {
        if (GatewayMessageHelper::IsFuncCallComplete(message)) {
            func_call_context->set_status(FuncCallContext::kSuccess);
            func_call_context->append_output(payload);
        } else if (GatewayMessageHelper::IsFuncCallFailed(message)) {
            func_call_context->set_status(FuncCallContext::kFailed);
        } else {
            UNREACHABLE();
        }
        FinishFuncCall(std::move(parent_connection), func_call_context);
    }
    TryDispatchingPendingFuncCalls();
}

void Server::OnRecvEngineMessage(EngineConnection* connection, const GatewayMessage& message,
                                 std::span<const char> payload) {
    if (GatewayMessageHelper::IsFuncCallComplete(message)
            || GatewayMessageHelper::IsFuncCallFailed(message)) {
        HandleFuncCallCompleteOrFailedMessage(connection->node_id(), message, payload);
    } else {
        HLOG(ERROR) << "Unknown engine message type";
    }
}

Server::PerFuncStat::PerFuncStat(uint16_t func_id)
    : last_request_timestamp(-1),
      incoming_requests_stat(stat::Counter::StandardReportCallback(
          fmt::format("incoming_requests[{}]", func_id))),
      request_interval_stat(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("request_interval[{}]", func_id))) {}

void Server::TickNewFuncCall(uint16_t func_id, int64_t current_timestamp) {
    if (!per_func_stats_.contains(func_id)) {
        per_func_stats_[func_id] = std::unique_ptr<PerFuncStat>(new PerFuncStat(func_id));
    }
    PerFuncStat* per_func_stat = per_func_stats_[func_id].get();
    per_func_stat->incoming_requests_stat.Tick();
    if (current_timestamp <= per_func_stat->last_request_timestamp) {
        current_timestamp = per_func_stat->last_request_timestamp + 1;
    }
    if (last_request_timestamp_ != -1) {
        per_func_stat->request_interval_stat.AddSample(gsl::narrow_cast<int32_t>(
            current_timestamp - per_func_stat->last_request_timestamp));
    }
    per_func_stat->last_request_timestamp = current_timestamp;
}

void Server::OnNewFuncCallCommon(std::shared_ptr<server::ConnectionBase> parent_connection,
                                 FuncCallContext* func_call_context) {
    FuncCall func_call = func_call_context->func_call();
    FuncCallState state = {
        .func_call = func_call,
        .connection_id = parent_connection->id(),
        .context = func_call_context,
        .recv_timestamp = 0,
        .dispatch_timestamp = 0
    };
    uint16_t node_id;
    bool node_picked = node_manager_.PickNodeForNewFuncCall(func_call, &node_id);
    {
        absl::MutexLock lk(&mu_);
        int64_t current_timestamp = GetMonotonicMicroTimestamp();
        state.recv_timestamp = current_timestamp;
        incoming_requests_stat_.Tick();
        if (current_timestamp <= last_request_timestamp_) {
            current_timestamp = last_request_timestamp_ + 1;
        }
        if (last_request_timestamp_ != -1) {
            requests_instant_rps_stat_.AddSample(gsl::narrow_cast<float>(
                1e6 / (current_timestamp - last_request_timestamp_)));
            request_interval_stat_.AddSample(gsl::narrow_cast<int32_t>(
                current_timestamp - last_request_timestamp_));
        }
        last_request_timestamp_ = current_timestamp;
        TickNewFuncCall(func_call.func_id, current_timestamp);
        if (!node_picked) {
            pending_func_calls_.push_back(std::move(state));
        }
    }
    if (node_picked && DispatchFuncCall(std::move(parent_connection),
                                        func_call_context, node_id)) {
        absl::MutexLock lk(&mu_);
        state.dispatch_timestamp = state.recv_timestamp;
        running_func_calls_[func_call.full_call_id] = std::move(state);
        running_requests_stat_.AddSample(
            gsl::narrow_cast<uint16_t>(running_func_calls_.size()));
    }
}

bool Server::DispatchFuncCall(std::shared_ptr<server::ConnectionBase> parent_connection,
                              FuncCallContext* func_call_context, uint16_t node_id) {
    FuncCall func_call = func_call_context->func_call();
    GatewayMessage dispatch_message = GatewayMessageHelper::NewDispatchFuncCall(func_call);
    dispatch_message.payload_size = func_call_context->input().size();
    bool success = node_manager_.SendMessage(node_id, dispatch_message, func_call_context->input());
    if (!success) {
        node_manager_.FuncCallFinished(func_call, node_id);
        func_call_context->set_status(FuncCallContext::kNotFound);
        FinishFuncCall(std::move(parent_connection), func_call_context);
    }
    return success;
}

void Server::FinishFuncCall(std::shared_ptr<server::ConnectionBase> parent_connection,
                            FuncCallContext* func_call_context) {
    switch (parent_connection->type()) {
    case HttpConnection::kTypeId:
        parent_connection->as_ptr<HttpConnection>()->OnFuncCallFinished(func_call_context);
        break;
    case GrpcConnection::kTypeId:
        parent_connection->as_ptr<GrpcConnection>()->OnFuncCallFinished(func_call_context);
        break;
    default:
        UNREACHABLE();
    }
}

bool Server::OnEngineHandshake(uv_tcp_t* uv_handle, std::span<const char> data) {
    DCHECK_IN_EVENT_LOOP_THREAD(uv_loop());
    DCHECK_GE(data.size(), sizeof(GatewayMessage));
    const GatewayMessage* message = reinterpret_cast<const GatewayMessage*>(data.data());
    if (!GatewayMessageHelper::IsEngineHandshake(*message)) {
        HLOG(ERROR) << "Unexpected engine handshake message";
        return false;
    }
    uint16_t node_id = message->node_id;
    uint16_t conn_id = message->conn_id;
    std::span<const char> remaining_data(data.data() + sizeof(GatewayMessage),
                                         data.size() - sizeof(GatewayMessage));
    std::shared_ptr<server::ConnectionBase> connection(
        new EngineConnection(this, node_id, conn_id, remaining_data));
    size_t worker_id = conn_id % io_workers_.size();
    HLOG(INFO) << fmt::format("New engine connection (node_id={}, conn_id={}), "
                              "assigned to IO worker {}", node_id, conn_id, worker_id);
    server::IOWorker* io_worker = io_workers_[worker_id];
    RegisterConnection(io_worker, connection.get(), UV_AS_STREAM(uv_handle));
    DCHECK_GE(connection->id(), 0);
    DCHECK(!engine_connections_.contains(connection->id()));
    engine_connections_[connection->id()] = std::move(connection);
    return true;
}

UV_CONNECTION_CB_FOR_CLASS(Server, HttpConnection) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to open HTTP connection: " << uv_strerror(status);
        return;
    }
    std::shared_ptr<server::ConnectionBase> connection(
        new HttpConnection(this, next_http_connection_id_++));
    uv_tcp_t* client = reinterpret_cast<uv_tcp_t*>(malloc(sizeof(uv_tcp_t)));
    UV_DCHECK_OK(uv_tcp_init(uv_loop(), client));
    if (uv_accept(UV_AS_STREAM(&uv_http_handle_), UV_AS_STREAM(client)) == 0) {
        DCHECK_LT(next_http_conn_worker_id_, io_workers_.size());
        server::IOWorker* io_worker = io_workers_[next_http_conn_worker_id_];
        next_http_conn_worker_id_ = (next_http_conn_worker_id_ + 1) % io_workers_.size();
        RegisterConnection(io_worker, connection.get(), UV_AS_STREAM(client));
        DCHECK_GE(connection->id(), 0);
        {
            absl::MutexLock lk(&mu_);
            DCHECK(!connections_.contains(connection->id()));
            connections_[connection->id()] = std::move(connection);
        }
    } else {
        LOG(ERROR) << "Failed to accept new HTTP connection";
        free(client);
    }
}

UV_CONNECTION_CB_FOR_CLASS(Server, GrpcConnection) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to open gRPC connection: " << uv_strerror(status);
        return;
    }
    std::shared_ptr<server::ConnectionBase> connection(
        new GrpcConnection(this, next_grpc_connection_id_++));
    uv_tcp_t* client = reinterpret_cast<uv_tcp_t*>(malloc(sizeof(uv_tcp_t)));
    UV_DCHECK_OK(uv_tcp_init(uv_loop(), client));
    if (uv_accept(UV_AS_STREAM(&uv_grpc_handle_), UV_AS_STREAM(client)) == 0) {
        DCHECK_LT(next_grpc_conn_worker_id_, io_workers_.size());
        server::IOWorker* io_worker = io_workers_[next_grpc_conn_worker_id_];
        next_grpc_conn_worker_id_ = (next_grpc_conn_worker_id_ + 1) % io_workers_.size();
        RegisterConnection(io_worker, connection.get(), UV_AS_STREAM(client));
        DCHECK_GE(connection->id(), 0);
        {
            absl::MutexLock lk(&mu_);
            DCHECK(!connections_.contains(connection->id()));
            connections_[connection->id()] = std::move(connection);
        }
    } else {
        LOG(ERROR) << "Failed to accept new gRPC connection";
        free(client);
    }
}

class Server::OngoingEngineHandshake : public uv::Base {
public:
    OngoingEngineHandshake(Server* server, uv_tcp_t* uv_handle);
    ~OngoingEngineHandshake();

private:
    Server* server_;
    uv_tcp_t* uv_handle_;
    utils::AppendableBuffer read_buffer_;

    void OnReadHandshakeMessage();
    void OnReadError();

    DECLARE_UV_ALLOC_CB_FOR_CLASS(BufferAlloc);
    DECLARE_UV_READ_CB_FOR_CLASS(ReadMessage);

    DISALLOW_COPY_AND_ASSIGN(OngoingEngineHandshake);
};

UV_CONNECTION_CB_FOR_CLASS(Server, EngineConnection) {
    if (status != 0) {
        HLOG(WARNING) << "Failed to open engine connection: " << uv_strerror(status);
        return;
    }
    uv_tcp_t* client = reinterpret_cast<uv_tcp_t*>(malloc(sizeof(uv_tcp_t)));
    UV_DCHECK_OK(uv_tcp_init(uv_loop(), client));
    if (uv_accept(UV_AS_STREAM(&uv_engine_conn_handle_), UV_AS_STREAM(client)) == 0) {
        ongoing_engine_handshakes_.insert(std::unique_ptr<OngoingEngineHandshake>(
            new OngoingEngineHandshake(this, client)));
    } else {
        LOG(ERROR) << "Failed to accept new engine connection";
        free(client);
    }
}

Server::OngoingEngineHandshake::OngoingEngineHandshake(Server* server, uv_tcp_t* uv_handle)
    : server_(server), uv_handle_(uv_handle) {
    uv_handle_->data = this;
    UV_DCHECK_OK(uv_read_start(UV_AS_STREAM(uv_handle_),
                               &OngoingEngineHandshake::BufferAllocCallback,
                               &OngoingEngineHandshake::ReadMessageCallback));
}

Server::OngoingEngineHandshake::~OngoingEngineHandshake() {
    if (uv_handle_ != nullptr) {
        LOG(INFO) << "Close uv_handle for dead engine connection";
        uv_close(UV_AS_HANDLE(uv_handle_), uv::HandleFreeCallback);
    }
}

void Server::OngoingEngineHandshake::OnReadHandshakeMessage() {
    if (server_->OnEngineHandshake(uv_handle_, read_buffer_.to_span())) {
        uv_handle_ = nullptr;
    }
    server_->ongoing_engine_handshakes_.erase(this);
}

void Server::OngoingEngineHandshake::OnReadError() {
    server_->ongoing_engine_handshakes_.erase(this);
}

UV_ALLOC_CB_FOR_CLASS(Server::OngoingEngineHandshake, BufferAlloc) {
    server_->read_buffer_pool_.Get(&buf->base, &buf->len);
}

UV_READ_CB_FOR_CLASS(Server::OngoingEngineHandshake, ReadMessage) {
    auto reclaim_resource = gsl::finally([server = server_, buf] {
        if (buf->base != 0) {
            server->read_buffer_pool_.Return(buf->base);
        }
    });
    if (nread < 0) {
        HLOG(ERROR) << "Read error on handshake: " << uv_strerror(nread);
        OnReadError();
        return;
    }
    if (nread == 0) {
        HLOG(WARNING) << "nread=0, will do nothing";
        return;
    }
    read_buffer_.AppendData(buf->base, nread);
    if (read_buffer_.length() >= sizeof(GatewayMessage)) {
        UV_DCHECK_OK(uv_read_stop(UV_AS_STREAM(uv_handle_)));
        OnReadHandshakeMessage();
    }
}

}  // namespace gateway
}  // namespace faas
