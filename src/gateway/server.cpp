#include "gateway/server.h"

#include "ipc/base.h"
#include "ipc/shm_region.h"
#include "common/time.h"
#include "common/zk_utils.h"
#include "utils/fs.h"
#include "utils/io.h"
#include "utils/socket.h"
#include "utils/docker.h"
#include "worker/worker_lib.h"
#include "gateway/flags.h"
#include "gateway/constants.h"

#define log_header_ "Server: "

namespace faas {
namespace gateway {

using protocol::FuncCall;
using protocol::FuncCallHelper;
using protocol::GatewayMessage;
using protocol::GatewayMessageHelper;

Server::Server()
    : http_port_(-1),
      grpc_port_(-1),
      message_sockfd_(-1),
      http_sockfd_(-1),
      grpc_sockfd_(-1),
      next_http_conn_worker_id_(0),
      next_grpc_conn_worker_id_(0),
      next_http_connection_id_(0),
      next_grpc_connection_id_(0),
      node_manager_(this),
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
          stat::StatisticsCollector<int32_t>::StandardReportCallback("dispatch_overhead")) {}

Server::~Server() {}

void Server::StartInternal() {
    // Load function config file
    std::string func_config_json;
    CHECK(!func_config_file_.empty());
    CHECK(fs_utils::ReadContents(func_config_file_, &func_config_json))
        << "Failed to read from file " << func_config_file_;
    CHECK(func_config_.Load(func_config_json));
    // Start IO workers
    int num_io_workers = absl::GetFlag(FLAGS_num_io_workers);
    CHECK_GT(num_io_workers, 0);
    HLOG(INFO) << fmt::format("Start {} IO workers", num_io_workers);
    for (int i = 0; i < num_io_workers; i++) {
        auto io_worker = CreateIOWorker(fmt::format("IO-{}", i));
        io_workers_.push_back(io_worker);
    }
    SetupHttpServer();
    if (grpc_port_ != -1) {
        SetupGrpcServer();
    }
    SetupMessageServer();
    // NodeManager starts to watch for engine nodes
    node_manager_.StartWatchingEngineNodes(zk_session());
}

void Server::StopInternal() {
    if (message_sockfd_ != -1) {
        PCHECK(close(message_sockfd_) == 0) << "Failed to close message server fd";
    }
    if (http_sockfd_ != -1) {
        PCHECK(close(http_sockfd_) == 0) << "Failed to close HTTP server fd";
    }
    if (grpc_sockfd_ != -1) {
        PCHECK(close(grpc_sockfd_) == 0) << "Failed to close gRPC server fd";
    }
}

void Server::OnConnectionClose(server::ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    int conn_type = (connection->type() & kConnectionTypeMask);
    if (conn_type == kHttpConnectionTypeId
          || conn_type == kGrpcConnectionTypeId) {
        absl::MutexLock lk(&mu_);
        DCHECK(connections_.contains(connection->id()));
        connections_.erase(connection->id());
    } else if (conn_type == kEngineIngressTypeId) {
        absl::MutexLock lk(&mu_);
        DCHECK(engine_ingress_conns_.contains(connection->id()));
        engine_ingress_conns_.erase(connection->id());
    } else if (conn_type == kEngineEgressHubTypeId) {
        absl::MutexLock lk(&mu_);
        DCHECK(engine_egress_hubs_.contains(connection->id()));
        engine_egress_hubs_.erase(connection->id());
    } else {
        UNREACHABLE();
    }
}

void Server::SetupHttpServer() {
    // Listen on address:http_port for HTTP requests
    std::string address = absl::GetFlag(FLAGS_listen_addr);
    CHECK(!address.empty());
    CHECK_NE(http_port_, -1);
    http_sockfd_ = utils::TcpSocketBindAndListen(
        address, http_port_, absl::GetFlag(FLAGS_socket_listen_backlog));
    CHECK(http_sockfd_ != -1)
        << fmt::format("Failed to listen on {}:{}", address, http_port_);
    HLOG(INFO) << fmt::format("Listen on {}:{} for HTTP requests", address, http_port_);
    ListenForNewConnections(
        http_sockfd_, absl::bind_front(&Server::OnNewHttpConnection, this));
}

void Server::SetupGrpcServer() {
    // Listen on address:grpc_port for gRPC requests
    std::string address = absl::GetFlag(FLAGS_listen_addr);
    CHECK(!address.empty());
    CHECK_NE(grpc_port_, -1);
    grpc_sockfd_ = utils::TcpSocketBindAndListen(
        address, grpc_port_, absl::GetFlag(FLAGS_socket_listen_backlog));
    CHECK(grpc_sockfd_ != -1)
        << fmt::format("Failed to listen on {}:{}", address, grpc_port_);
    HLOG(INFO) << fmt::format("Listen on {}:{} for gRPC requests", address, grpc_port_);
    ListenForNewConnections(
        grpc_sockfd_, absl::bind_front(&Server::OnNewGrpcConnection, this));
}

void Server::SetupMessageServer() {
    // Listen on address:message_port for message connections
    std::string address = absl::GetFlag(FLAGS_listen_addr);
    CHECK(!address.empty());
    uint16_t message_port;
    message_sockfd_ = utils::TcpSocketBindArbitraryPort(address, &message_port);
    CHECK(message_sockfd_ != -1)
        << fmt::format("Failed to bind on {}", address);
    CHECK(utils::SocketListen(message_sockfd_, absl::GetFlag(FLAGS_socket_listen_backlog)))
        << fmt::format("Failed to listen on {}:{}", address, message_port);
    HLOG(INFO) << fmt::format("Listen on {}:{} for message connections",
                              address, message_port);
    ListenForNewConnections(
        message_sockfd_, absl::bind_front(&Server::OnNewMessageConnection, this));
    // Save gateway host address to ZooKeeper for engines to connect
    std::string gateway_addr(
        fmt::format("{}:{}", absl::GetFlag(FLAGS_hostname), message_port));
    auto status = zk_utils::CreateSync(
        zk_session(), /* path= */ "gateway",
        /* value= */ STRING_TO_SPAN(gateway_addr),
        zk::ZKCreateMode::kEphemeral, nullptr);
    CHECK(status.ok()) << "Failed to create ZooKeeper node: " << status.ToString();
}

void Server::OnNewHttpFuncCall(HttpConnection* connection, FuncCallContext* func_call_context) {
    auto func_entry = func_config_.find_by_func_name(func_call_context->func_name());
    if (func_entry == nullptr) {
        func_call_context->set_status(FuncCallContext::kNotFound);
        connection->OnFuncCallFinished(func_call_context);
        return;
    }
    uint32_t call_id = next_call_id_.fetch_add(1, std::memory_order_relaxed);
    FuncCall func_call = FuncCallHelper::New(gsl::narrow_cast<uint16_t>(func_entry->func_id),
                                             /* client_id= */ 0, call_id);
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
    uint32_t call_id = next_call_id_.fetch_add(1, std::memory_order_relaxed);
    FuncCall func_call = FuncCallHelper::NewWithMethod(
        gsl::narrow_cast<uint16_t>(func_entry->func_id),
        gsl::narrow_cast<uint16_t>(func_entry->grpc_method_ids.at(method_name)),
        /* client_id= */ 0, call_id);
    VLOG(1) << "OnNewGrpcFuncCall: " << FuncCallHelper::DebugString(func_call);
    func_call_context->set_func_call(func_call);
    OnNewFuncCallCommon(connection->ref_self(), func_call_context);
}

void Server::DiscardFuncCall(FuncCallContext* func_call_context) {
    absl::MutexLock lk(&mu_);
    discarded_func_calls_.insert(func_call_context->func_call().full_call_id);
}

void Server::OnEngineNodeOnline(uint16_t node_id) {
    HLOG(INFO) << fmt::format("Engine node {} is online", node_id);
    TryDispatchingPendingFuncCalls();
}

void Server::OnEngineNodeOffline(uint16_t node_id) {
    HLOG(INFO) << fmt::format("Engine node {} is offline", node_id);
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
        bool async_call = (state.connection_id == -1);
        std::shared_ptr<server::ConnectionBase> parent_connection;
        if (!async_call) {
            if (!connections_.contains(state.connection_id)) {
                continue;
            }
            parent_connection = connections_[state.connection_id];
        }
        mu_.Unlock();
        uint16_t node_id;
        bool node_picked = node_manager_.PickNodeForNewFuncCall(func_call, &node_id);
        bool dispatched = false;
        if (node_picked) {
            if (async_call) {
                dispatched = DispatchAsyncFuncCall(func_call, STRING_TO_SPAN(state.input),
                                                   node_id);
            } else {
                dispatched = DispatchFuncCall(std::move(parent_connection),
                                              state.context, node_id);
            }
        }
        mu_.Lock();
        if (!node_picked) {
            pending_func_calls_.push_front(std::move(state));
            break;
        }
        state.dispatch_timestamp = GetMonotonicMicroTimestamp();
        queueing_delay_stat_.AddSample(gsl::narrow_cast<int32_t>(
            state.dispatch_timestamp - state.recv_timestamp));
        if (dispatched) {
            running_func_calls_[func_call.full_call_id] = std::move(state);
            running_requests_stat_.AddSample(
                gsl::narrow_cast<uint16_t>(running_func_calls_.size()));
        }
    }
    mu_.Unlock();
}

bool Server::SendMessageToEngine(uint16_t node_id, const GatewayMessage& message,
                                 std::span<const char> payload) {
    server::IOWorker* io_worker = server::IOWorker::current();
    server::ConnectionBase* conn = io_worker->PickConnection(
        kEngineEgressHubTypeId + node_id);
    server::EgressHub* hub = nullptr;
    if (conn == nullptr) {
        struct sockaddr_in addr;
        if (!node_manager_.GetNodeAddr(node_id, &addr)) {
            return false;
        }
        auto egress_hub = std::make_unique<server::EgressHub>(
            kEngineEgressHubTypeId + node_id,
            &addr, absl::GetFlag(FLAGS_message_conn_per_worker));
        egress_hub->set_log_header(fmt::format("EngineEgressHub[{}]: ", node_id));
        egress_hub->SetHandshakeMessageCallback([] (std::string* handshake) {
            *handshake = protocol::EncodeHandshakeMessage(
                protocol::ConnType::GATEWAY_TO_ENGINE);
        });
        RegisterConnection(io_worker, egress_hub.get());
        DCHECK_GE(egress_hub->id(), 0);
        {
            absl::MutexLock lk(&mu_);
            DCHECK(!engine_egress_hubs_.contains(egress_hub->id()));
            engine_egress_hubs_[egress_hub->id()] = std::move(egress_hub);
        }
        hub = egress_hub.get();
    } else {
        hub = conn->as_ptr<server::EgressHub>();
    }
    DCHECK(hub != nullptr);
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(GatewayMessage));
    hub->SendMessage(data, payload);
    return true;
}

void Server::HandleFuncCallCompleteOrFailedMessage(uint16_t node_id,
                                                   const GatewayMessage& message,
                                                   std::span<const char> payload) {
    DCHECK(GatewayMessageHelper::IsFuncCallComplete(message)
             || GatewayMessageHelper::IsFuncCallFailed(message));
    FuncCall func_call = GatewayMessageHelper::GetFuncCall(message);
    node_manager_.FuncCallFinished(func_call, node_id);
    bool async_call = false;
    FuncCallContext* func_call_context = nullptr;
    std::shared_ptr<server::ConnectionBase> parent_connection;
    {
        absl::MutexLock lk(&mu_);
        if (!running_func_calls_.contains(func_call.full_call_id)) {
            HLOG(ERROR) << "Cannot find running FuncCall: "
                        << FuncCallHelper::DebugString(func_call);
            return;
        }
        const FuncCallState& state = running_func_calls_[func_call.full_call_id];
        if (state.connection_id == -1) {
            async_call = true;
        }
        if (!async_call && !discarded_func_calls_.contains(func_call.full_call_id)) {
            // Check if corresponding connection is still active
            if (connections_.contains(state.connection_id)) {
                parent_connection = connections_[state.connection_id];
                func_call_context = state.context;
            }
        }
        if (discarded_func_calls_.contains(func_call.full_call_id)) {
            discarded_func_calls_.erase(func_call.full_call_id);
        }
        int64_t current_timestamp = GetMonotonicMicroTimestamp();
        dispatch_overhead_stat_.AddSample(gsl::narrow_cast<int32_t>(
            current_timestamp - state.dispatch_timestamp - message.processing_time));
        if (async_call && GatewayMessageHelper::IsFuncCallComplete(message)) {
            uint16_t func_id = func_call.func_id;
            DCHECK(per_func_stats_.contains(func_id));
            PerFuncStat* per_func_stat = per_func_stats_[func_id].get();
            per_func_stat->end2end_delay_stat.AddSample(gsl::narrow_cast<int32_t>(
                current_timestamp - state.recv_timestamp));
        }
        running_func_calls_.erase(func_call.full_call_id);
    }
    if (async_call) {
        if (GatewayMessageHelper::IsFuncCallFailed(message)) {
            auto func_entry = func_config_.find_by_func_id(func_call.func_id);
            HLOG(WARNING) << fmt::format("Async call of {} failed",
                                         DCHECK_NOTNULL(func_entry)->func_name);
        }
    } else if (func_call_context != nullptr) {
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

void Server::OnRecvEngineMessage(uint16_t node_id, const GatewayMessage& message,
                                 std::span<const char> payload) {
    if (GatewayMessageHelper::IsFuncCallComplete(message)
            || GatewayMessageHelper::IsFuncCallFailed(message)) {
        HandleFuncCallCompleteOrFailedMessage(node_id, message, payload);
    } else {
        HLOG(ERROR) << "Unknown engine message type";
    }
}

Server::PerFuncStat::PerFuncStat(uint16_t func_id)
    : last_request_timestamp(-1),
      incoming_requests_stat(stat::Counter::StandardReportCallback(
          fmt::format("incoming_requests[{}]", func_id))),
      request_interval_stat(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("request_interval[{}]", func_id))),
      end2end_delay_stat(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("end2end_delay[{}]", func_id))) {}

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
        .connection_id = func_call_context->is_async() ? -1 : parent_connection->id(),
        .context = func_call_context->is_async() ? nullptr : func_call_context,
        .recv_timestamp = 0,
        .dispatch_timestamp = 0,
        .input = std::string()
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
            if (func_call_context->is_async()) {
                // Make a copy of input in state
                std::span<const char> input = func_call_context->input();
                state.input.assign(input.data(), input.size());
            }
            pending_func_calls_.push_back(std::move(state));
        }
    }
    bool dispatched = false;
    if (func_call_context->is_async()) {
        if (!node_picked) {
            func_call_context->set_status(FuncCallContext::kSuccess);
        } else if (DispatchAsyncFuncCall(func_call, func_call_context->input(), node_id)) {
            dispatched = true;
            func_call_context->set_status(FuncCallContext::kSuccess);
        } else {
            func_call_context->set_status(FuncCallContext::kNotFound);
        }
        FinishFuncCall(std::move(parent_connection), func_call_context);
    } else if (node_picked && DispatchFuncCall(std::move(parent_connection),
                                               func_call_context, node_id)) {
        dispatched = true;
    }
    if (dispatched) {
        DCHECK(node_picked);
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
    bool success = SendMessageToEngine(node_id, dispatch_message, func_call_context->input());
    if (!success) {
        node_manager_.FuncCallFinished(func_call, node_id);
        func_call_context->set_status(FuncCallContext::kNotFound);
        FinishFuncCall(std::move(parent_connection), func_call_context);
    }
    return success;
}

bool Server::DispatchAsyncFuncCall(const FuncCall& func_call, std::span<const char> input,
                                   uint16_t node_id) {
    GatewayMessage dispatch_message = GatewayMessageHelper::NewDispatchFuncCall(func_call);
    dispatch_message.payload_size = input.size();
    bool success = SendMessageToEngine(node_id, dispatch_message, input);
    if (!success) {
        node_manager_.FuncCallFinished(func_call, node_id);
    }
    return success;
}

void Server::FinishFuncCall(std::shared_ptr<server::ConnectionBase> parent_connection,
                            FuncCallContext* func_call_context) {
    switch (parent_connection->type() & kConnectionTypeMask) {
    case kHttpConnectionTypeId:
        parent_connection->as_ptr<HttpConnection>()->OnFuncCallFinished(func_call_context);
        break;
    case kGrpcConnectionTypeId:
        parent_connection->as_ptr<GrpcConnection>()->OnFuncCallFinished(func_call_context);
        break;
    default:
        UNREACHABLE();
    }
}

void Server::OnNewMessageConnection(int sockfd) {
    protocol::HandshakeMessage handshake;
    if (!io_utils::RecvMessage(sockfd, &handshake, nullptr)) {
        HPLOG(ERROR) << "Failed to read handshake message from engine";
        close(sockfd);
        return;
    }
    protocol::ConnType conn_type = static_cast<protocol::ConnType>(handshake.conn_type);
    if (conn_type != protocol::ConnType::ENGINE_TO_GATEWAY) {
        HLOG(ERROR) << "Invalid connection type: " << handshake.conn_type;
        close(sockfd);
        return;
    }

    uint16_t node_id = handshake.src_node_id;
    auto connection = std::make_unique<server::IngressConnection>(
        kEngineIngressTypeId + node_id, sockfd, sizeof(GatewayMessage));
    connection->set_log_header(fmt::format("EngineIngress[{}]: ", node_id));
    connection->SetMessageFullSizeCallback(
        [] (std::span<const char> header) -> size_t {
            DCHECK_EQ(header.size(), sizeof(GatewayMessage));
            const GatewayMessage* message = reinterpret_cast<const GatewayMessage*>(
                header.data());
            DCHECK_GE(message->payload_size, 0);
            return message->payload_size;
        }
    );
    connection->SetNewMessageCallback(
        [this, node_id] (std::span<const char> data) {
            DCHECK_GE(data.size(), sizeof(GatewayMessage));
            const GatewayMessage* message = reinterpret_cast<const GatewayMessage*>(
                data.data());
            std::span<const char> payload;
            if (data.size() > sizeof(GatewayMessage)) {
                payload = data.subspan(sizeof(GatewayMessage));
            }
            OnRecvEngineMessage(node_id, *message, payload);
        }
    );

    size_t idx = (next_engine_conn_worker_id_[node_id]++) % io_workers_.size();
    server::IOWorker* io_worker = io_workers_[idx];
    RegisterConnection(io_worker, connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!engine_ingress_conns_.contains(connection->id()));
    engine_ingress_conns_[connection->id()] = std::move(connection);
}

void Server::OnNewHttpConnection(int sockfd) {
    std::shared_ptr<server::ConnectionBase> connection(
        new HttpConnection(this, next_http_connection_id_++, sockfd));
    size_t idx = (next_http_conn_worker_id_++) % io_workers_.size();
    server::IOWorker* io_worker = io_workers_[idx];
    RegisterConnection(io_worker, connection.get());
    DCHECK_GE(connection->id(), 0);
    {
        absl::MutexLock lk(&mu_);
        DCHECK(!connections_.contains(connection->id()));
        connections_[connection->id()] = std::move(connection);
    }
}

void Server::OnNewGrpcConnection(int sockfd) {
    std::shared_ptr<server::ConnectionBase> connection(
        new GrpcConnection(this, next_grpc_connection_id_++, sockfd));
    size_t idx = (next_grpc_conn_worker_id_++) % io_workers_.size();
    server::IOWorker* io_worker = io_workers_[idx];
    RegisterConnection(io_worker, connection.get());
    DCHECK_GE(connection->id(), 0);
    {
        absl::MutexLock lk(&mu_);
        DCHECK(!connections_.contains(connection->id()));
        connections_[connection->id()] = std::move(connection);
    }
}

}  // namespace gateway
}  // namespace faas
