#include "engine/engine.h"

#include "ipc/base.h"
#include "ipc/shm_region.h"
#include "common/time.h"
#include "common/zk_utils.h"
#include "utils/fs.h"
#include "utils/io.h"
#include "utils/docker.h"
#include "utils/socket.h"
#include "worker/worker_lib.h"
#include "engine/flags.h"
#include "engine/constants.h"
#include "engine/sequencer_connection.h"

#define log_header_ "Engine: "

namespace faas {
namespace engine {

using protocol::FuncCall;
using protocol::Message;
using protocol::MessageHelper;
using protocol::GatewayMessage;
using protocol::GatewayMessageHelper;
using protocol::SharedLogOpType;

Engine::Engine()
    : engine_tcp_port_(-1),
      enable_shared_log_(false),
      shared_log_tcp_port_(-1),
      func_worker_use_engine_socket_(absl::GetFlag(FLAGS_func_worker_use_engine_socket)),
      use_fifo_for_nested_call_(absl::GetFlag(FLAGS_use_fifo_for_nested_call)),
      message_sockfd_(-1),
      ipc_sockfd_(-1),
      shared_log_sockfd_(-1),
      next_ipc_conn_worker_id_(0),
      next_shared_log_conn_worker_id_(0),
      worker_manager_(new WorkerManager(this)),
      monitor_(absl::GetFlag(FLAGS_enable_monitor) ? new Monitor(this) : nullptr),
      tracer_(new Tracer(this)),
      inflight_external_requests_(0),
      last_external_request_timestamp_(-1),
      incoming_external_requests_stat_(
          stat::Counter::StandardReportCallback("incoming_external_requests")),
      incoming_internal_requests_stat_(
          stat::Counter::StandardReportCallback("incoming_internal_requests")),
      external_requests_instant_rps_stat_(
          stat::StatisticsCollector<float>::StandardReportCallback("external_requests_instant_rps")),
      inflight_external_requests_stat_(
          stat::StatisticsCollector<uint16_t>::StandardReportCallback("inflight_external_requests")),
      message_delay_stat_(
          stat::StatisticsCollector<int32_t>::StandardReportCallback("message_delay")),
      input_use_shm_stat_(stat::Counter::StandardReportCallback("input_use_shm")),
      output_use_shm_stat_(stat::Counter::StandardReportCallback("output_use_shm")),
      discarded_func_call_stat_(stat::Counter::StandardReportCallback("discarded_func_call")) {}

Engine::~Engine() {}

void Engine::StartInternal() {
    // Load function config file
    CHECK(!func_config_file_.empty());
    CHECK(fs_utils::ReadContents(func_config_file_, &func_config_json_))
        << "Failed to read from file " << func_config_file_;
    CHECK(func_config_.Load(func_config_json_));
    // Start IO workers
    int num_io_workers = absl::GetFlag(FLAGS_num_io_workers);
    CHECK_GT(num_io_workers, 0);
    HLOG(INFO) << fmt::format("Start {} IO workers", num_io_workers);
    for (int i = 0; i < num_io_workers; i++) {
        auto io_worker = CreateIOWorker(fmt::format("IO-{}", i));
        io_workers_.push_back(io_worker);
    }
    SetupGatewayEgress();
    SetupLocalIpc();
    if (enable_shared_log_) {
        SetupSharedLog();
    }
    SetupMessageServer();
    // Initialize tracer
    tracer_->Init();
}

void Engine::SetupGatewayEgress() {
    std::string gateway_addr;
    auto status = zk_utils::GetOrWaitSync(zk_session(), "gateway", &gateway_addr);
    CHECK(status.ok()) << "Failed to get gateway address from ZooKeeper: "
                       << status.ToString();
    struct sockaddr_in addr;
    if (!utils::ResolveTcpAddr(&addr, gateway_addr)) {
        HLOG(FATAL) << "Cannot resolve address for gateway: " << gateway_addr;
    }
    for (server::IOWorker* io_worker : io_workers_) {
        auto egress_hub = std::make_unique<server::EgressHub>(
            kGatewayEgressHubTypeId,
            &addr, absl::GetFlag(FLAGS_message_conn_per_worker));
        egress_hub->set_log_header("GatewayEgressHub: ");
        uint16_t node_id = node_id_;
        egress_hub->SetHandshakeMessageCallback([node_id] (std::string* handshake) {
            *handshake = protocol::EncodeHandshakeMessage(
                protocol::ConnType::ENGINE_TO_GATEWAY, node_id);
        });
        RegisterConnection(io_worker, egress_hub.get());
        DCHECK_GE(egress_hub->id(), 0);
        DCHECK(!gateway_egress_hubs_.contains(egress_hub->id()));
        gateway_egress_hubs_[egress_hub->id()] = std::move(egress_hub);
    }
}

void Engine::SetupLocalIpc() {
    int listen_backlog = absl::GetFlag(FLAGS_socket_listen_backlog);
    if (engine_tcp_port_ == -1) {
        std::string ipc_path(ipc::GetEngineUnixSocketPath());
        if (fs_utils::Exists(ipc_path)) {
            PCHECK(fs_utils::Remove(ipc_path));
        }
        ipc_sockfd_ = utils::UnixSocketBindAndListen(ipc_path, listen_backlog);
        CHECK(ipc_sockfd_ != -1)
            << fmt::format("Failed to listen on {}", ipc_path);
        HLOG(INFO) << fmt::format("Listen on {} for IPC connections", ipc_path);
    } else {
        std::string address = absl::GetFlag(FLAGS_listen_addr);
        CHECK(!address.empty());
        ipc_sockfd_ = utils::TcpSocketBindAndListen(
            address, engine_tcp_port_, listen_backlog);
        CHECK(ipc_sockfd_ != -1)
            << fmt::format("Failed to listen on {}:{}", address, engine_tcp_port_);
        HLOG(INFO) << fmt::format("Listen on {}:{} for IPC connections",
                                  address, engine_tcp_port_);
    }
    ListenForNewConnections(ipc_sockfd_, absl::bind_front(&Engine::OnNewLocalIpcConn, this));
}

void Engine::SetupMessageServer() {
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
        message_sockfd_, absl::bind_front(&Engine::OnNewRemoteMessageConn, this));
    // Save my host address to ZooKeeper for others to connect
    std::string my_addr(fmt::format("{}:{}", absl::GetFlag(FLAGS_hostname), message_port));
    std::string znode_path = fmt::format("engines/{}", node_id_);
    auto status = zk_utils::CreateSync(
        zk_session(), /* path= */ znode_path, /* value= */ STRING_TO_SPAN(my_addr),
        zk::ZKCreateMode::kEphemeral, nullptr);
    CHECK(status.ok()) << "Failed to create ZooKeeper node: " << status.ToString();
}

void Engine::SetupSharedLog() {
    DCHECK(enable_shared_log_);
    if (!sequencer_config_.LoadFromFile(sequencer_config_file_)) {
        HLOG(FATAL) << "Failed to load sequencer config";
    }
    slog_engine_.reset(new SLogEngine(this));
    // Listen on shared_log_tcp_port
    std::string address = absl::GetFlag(FLAGS_listen_addr);
    CHECK(!address.empty());
    CHECK_NE(shared_log_tcp_port_, -1);
    shared_log_sockfd_ = utils::TcpSocketBindAndListen(
        address, shared_log_tcp_port_, absl::GetFlag(FLAGS_socket_listen_backlog));
    CHECK(shared_log_sockfd_ != -1)
        << fmt::format("Failed to listen on {}:{}", address, shared_log_tcp_port_);
    HLOG(INFO) << fmt::format("Listen on {}:{} for shared log related connections",
                              address, shared_log_tcp_port_);
    ListenForNewConnections(shared_log_sockfd_,
                            absl::bind_front(&Engine::OnNewSLogConnection, this));
    // Connect to sequencer
    sequencer_config_.ForEachPeer([this] (const SequencerConfig::Peer* peer) {
        int total_conn = io_workers_.size() * absl::GetFlag(FLAGS_sequencer_conn_per_worker);
        for (int i = 0; i < total_conn; i++) {
            int sockfd = -1;
            bool success = utils::NetworkOpWithRetry(
                /* max_retry= */ 10, /* sleep_sec=*/ 3,
                [peer, &sockfd] {
                    sockfd = utils::TcpSocketConnect(peer->host_addr, peer->engine_conn_port);
                    return sockfd != -1;
                }
            );
            if (!success) {
                HLOG(FATAL) << fmt::format("Failed to connect to sequencer {}:{}",
                                           peer->host_addr, peer->engine_conn_port);
            }
            std::shared_ptr<server::ConnectionBase> connection(
                new SequencerConnection(this, slog_engine_.get(), peer->id, sockfd));
            server::IOWorker* io_worker = io_workers_[i % io_workers_.size()];
            RegisterConnection(io_worker, connection.get());
            DCHECK_GE(connection->id(), 0);
            DCHECK(!sequencer_connections_.contains(connection->id()));
            sequencer_connections_[connection->id()] = std::move(connection);
        }
    });
    // Setup SharedLogMessageHub for each server::IOWorker
    for (size_t i = 0; i < io_workers_.size(); i++) {
        auto hub = std::make_unique<SLogMessageHub>(slog_engine_.get());
        RegisterConnection(io_workers_[i], hub.get());
        slog_message_hubs_.insert(std::move(hub));
    }
}

void Engine::StopInternal() {
    if (message_sockfd_ != -1) {
        PCHECK(close(message_sockfd_) == 0) << "Failed to close message server fd";
    }
    if (ipc_sockfd_ != -1) {
        PCHECK(close(ipc_sockfd_) == 0) << "Failed to close local IPC server fd";
    }
    if (shared_log_sockfd_ != -1) {
        PCHECK(close(shared_log_sockfd_) == 0) << "Failed to close server fd";
    }
}

Timer* Engine::CreateTimer(int timer_type, server::IOWorker* io_worker, Timer::Callback cb) {
    Timer* timer = new Timer(timer_type, cb);
    RegisterConnection(io_worker, timer);
    timers_.insert(std::unique_ptr<Timer>(timer));
    return timer;
}

Timer* Engine::CreatePeriodicTimer(int timer_type, server::IOWorker* io_worker,
                                   absl::Time initial, absl::Duration duration,
                                   Timer::Callback cb) {
    Timer* timer = new Timer(timer_type, cb);
    timer->SetPeriodic(initial, duration);
    RegisterConnection(io_worker, timer);
    timers_.insert(std::unique_ptr<Timer>(timer));
    return timer;
}

void Engine::OnConnectionClose(server::ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    switch (connection->type() & kConnectionTypeMask) {
    case kMessageConnectionTypeId:
        DCHECK(message_connections_.contains(connection->id()));
        OnLocalIpcConnClosed(connection->as_ptr<MessageConnection>());
        message_connections_.erase(connection->id());
        break;
    case kGatewayIngressTypeId:
        DCHECK(gateway_ingress_conns_.contains(connection->id()));
        gateway_ingress_conns_.erase(connection->id());
        break;
    case kGatewayEgressHubTypeId:
        if (state_.load() != kStopping) {
            HLOG(ERROR) << "Lost connections to gateway";
            ScheduleStop();
        }
        break;
    case kSequencerConnectionTypeId:
        DCHECK(sequencer_connections_.contains(connection->id()));
        HLOG(WARNING) << "Sequencer connection disconencted";
        sequencer_connections_.erase(connection->id());
        break;
    case kIncomingSLogConnectionTypeId:
        DCHECK(slog_connections_.contains(connection->id()));
        slog_connections_.erase(connection->id());
        break;
    case kSLogMessageHubTypeId:
        if (state_.load() != kStopping) {
            HLOG(FATAL) << "SLogMessageHub should not be closed";
        }
        break;
    case kTimerTypeId:
        if (timers_.contains(connection->as_ptr<Timer>())) {
            timers_.erase(connection->as_ptr<Timer>());
        } else {
            HLOG(FATAL) << "Cannot find timer in timers_";
        }
        break;
    default:
        HLOG(FATAL) << "Unknown connection type: " << connection->type();
    }
}

bool Engine::OnNewHandshake(MessageConnection* connection,
                            const Message& handshake_message, Message* response,
                            std::span<const char>* response_payload) {
    if (!MessageHelper::IsLauncherHandshake(handshake_message)
          && !MessageHelper::IsFuncWorkerHandshake(handshake_message)) {
        HLOG(ERROR) << "Received message is not a handshake message";
        return false;
    }
    HLOG(INFO) << "Receive new handshake message from message connection";
    uint16_t func_id = handshake_message.func_id;
    if (func_config_.find_by_func_id(func_id) == nullptr) {
        HLOG(ERROR) << "Invalid func_id " << func_id << " in handshake message";
        return false;
    }
    bool success;
    if (MessageHelper::IsLauncherHandshake(handshake_message)) {
        std::span<const char> payload = MessageHelper::GetInlineData(handshake_message);
        if (payload.size() != docker_utils::kContainerIdLength) {
            HLOG(ERROR) << "Launcher handshake does not have container ID in inline data";
            return false;
        }
        std::string container_id(payload.data(), payload.size());
        if (monitor_ != nullptr && container_id != docker_utils::kInvalidContainerId) {
            monitor_->OnNewFuncContainer(func_id, container_id);
        }
        success = worker_manager_->OnLauncherConnected(connection);
    } else {
        success = worker_manager_->OnFuncWorkerConnected(connection);
        ProcessDiscardedFuncCallIfNecessary();
    }
    if (!success) {
        return false;
    }
    if (MessageHelper::IsLauncherHandshake(handshake_message)) {
        *response = MessageHelper::NewHandshakeResponse(func_config_json_.size());
        if (func_worker_use_engine_socket_) {
            response->flags |= protocol::kFuncWorkerUseEngineSocketFlag;
        }
        *response_payload = STRING_TO_SPAN(func_config_json_);
    } else {
        *response = MessageHelper::NewHandshakeResponse(0);
        if (use_fifo_for_nested_call_) {
            response->flags |= protocol::kUseFifoForNestedCallFlag;
        }
        *response_payload = std::span<const char>();
    }
    return true;
}

void Engine::OnLocalIpcConnClosed(MessageConnection* conn) {
    if (conn->handshake_done()) {
        if (conn->is_launcher_connection()) {
            worker_manager_->OnLauncherDisconnected(conn);
        } else {
            worker_manager_->OnFuncWorkerDisconnected(conn);
        }
    }
}

void Engine::OnRecvGatewayMessage(const GatewayMessage& message,
                                  std::span<const char> payload) {
    if (GatewayMessageHelper::IsDispatchFuncCall(message)) {
        FuncCall func_call = GatewayMessageHelper::GetFuncCall(message);
        OnExternalFuncCall(func_call, payload);
    } else {
        HLOG(ERROR) << "Unknown engine message type";
    }
}

void Engine::HandleInvokeFuncMessage(const Message& message) {
    DCHECK(MessageHelper::IsInvokeFunc(message));
    int32_t message_delay = MessageHelper::ComputeMessageDelay(message);
    FuncCall func_call = MessageHelper::GetFuncCall(message);
    FuncCall parent_func_call;
    parent_func_call.full_call_id = message.parent_call_id;
    bool is_async = (message.flags & protocol::kAsyncInvokeFuncFlag) != 0;
    AsyncFuncCall async_call = {
        .func_call = func_call,
        .parent_func_call = parent_func_call,
        .input_region = nullptr
    };
    if (is_async && message.payload_size < 0) {
        async_call.input_region = ipc::ShmOpen(
            ipc::GetFuncCallInputShmName(func_call.full_call_id));
        if (async_call.input_region == nullptr) {
            HLOG(WARNING) << "Cannot open input shm of new async FuncCall, "
                             "will not dispatch it";
            return;
        }
        async_call.input_region->EnableRemoveOnDestruction();
    }
    Dispatcher* dispatcher = nullptr;
    {
        absl::MutexLock lk(&mu_);
        incoming_internal_requests_stat_.Tick();
        if (message.payload_size < 0) {
            input_use_shm_stat_.Tick();
        }
        if (message_delay >= 0) {
            message_delay_stat_.AddSample(message_delay);
        }
        dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
        if (is_async) {
            async_func_calls_[func_call.full_call_id] = std::move(async_call);
        }
    }
    if (slog_engine_ != nullptr) {
        slog_engine_->OnNewInternalFuncCall(func_call, parent_func_call);
    }
    bool success = false;
    if (dispatcher != nullptr) {
        if (message.payload_size < 0) {
            success = dispatcher->OnNewFuncCall(
                func_call, is_async ? protocol::kInvalidFuncCall : parent_func_call,
                /* input_size= */ gsl::narrow_cast<size_t>(-message.payload_size),
                std::span<const char>(), /* shm_input= */ true);
            
        } else {
            success = dispatcher->OnNewFuncCall(
                func_call, is_async ? protocol::kInvalidFuncCall : parent_func_call,
                /* input_size= */ gsl::narrow_cast<size_t>(message.payload_size),
                MessageHelper::GetInlineData(message), /* shm_input= */ false);
        }
    }
    if (!success) {
        HLOG(ERROR) << "Dispatcher failed for func_id " << func_call.func_id;
        if (slog_engine_ != nullptr) {
            slog_engine_->OnFuncCallCompleted(func_call);
        }
        if (is_async) {
            absl::MutexLock lk(&mu_);
            async_func_calls_.erase(func_call.full_call_id);
        }
    }
}

void Engine::HandleFuncCallCompleteMessage(const Message& message) {
    DCHECK(MessageHelper::IsFuncCallComplete(message));
    int32_t message_delay = MessageHelper::ComputeMessageDelay(message);
    FuncCall func_call = MessageHelper::GetFuncCall(message);
    Dispatcher* dispatcher = nullptr;
    std::unique_ptr<ipc::ShmRegion> input_region = nullptr;
    bool is_async_call = false;
    AsyncFuncCall async_call;
    mu_.AssertNotHeld();
    {
        absl::MutexLock lk(&mu_);
        if (message_delay >= 0) {
            message_delay_stat_.AddSample(message_delay);
        }
        if ((func_call.client_id == 0 && message.payload_size < 0)
                || (func_call.client_id > 0 && message.payload_size + sizeof(int32_t) > PIPE_BUF)) {
            output_use_shm_stat_.Tick();
        }
        if (func_call.client_id == 0) {
            GrabFromMap(external_func_call_shm_inputs_, func_call, &input_region);
        }
        dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
        is_async_call = GrabFromMap(async_func_calls_, func_call, &async_call);
    }
    if (slog_engine_ != nullptr) {
        slog_engine_->OnFuncCallCompleted(func_call);
    }
    DCHECK(dispatcher != nullptr);
    bool ret = dispatcher->OnFuncCallCompleted(
        func_call, message.processing_time, message.dispatch_delay,
        /* output_size= */ gsl::narrow_cast<size_t>(std::abs(message.payload_size)));
    if (!ret) {
        HLOG(ERROR) << "Dispatcher::OnFuncCallCompleted failed";
        return;
    }
    if (func_call.client_id == 0) {
        if (message.payload_size < 0) {
            auto output_region = ipc::ShmOpen(
                ipc::GetFuncCallOutputShmName(func_call.full_call_id));
            if (output_region == nullptr) {
                ExternalFuncCallFailed(func_call);
            } else {
                output_region->EnableRemoveOnDestruction();
                ExternalFuncCallCompleted(func_call, output_region->to_span(),
                                          message.processing_time);
            }
        } else {
            ExternalFuncCallCompleted(func_call, MessageHelper::GetInlineData(message),
                                      message.processing_time);
        }
    } else if (is_async_call) {
        if (message.payload_size < 0) {
            auto output_region = ipc::ShmOpen(
                ipc::GetFuncCallOutputShmName(func_call.full_call_id));
            if (output_region != nullptr) {
                output_region->EnableRemoveOnDestruction();
            }
        }
    } else if (!use_fifo_for_nested_call_) {
        Message message_copy = message;
        SendFuncWorkerMessage(func_call.client_id, &message_copy);
    }
}

void Engine::HandleFuncCallFailedMessage(const Message& message) {
    DCHECK(MessageHelper::IsFuncCallFailed(message));
    int32_t message_delay = MessageHelper::ComputeMessageDelay(message);
    FuncCall func_call = MessageHelper::GetFuncCall(message);
    Dispatcher* dispatcher = nullptr;
    std::unique_ptr<ipc::ShmRegion> input_region = nullptr;
    bool is_async_call = false;
    AsyncFuncCall async_call;
    mu_.AssertNotHeld();
    {
        absl::MutexLock lk(&mu_);
        if (message_delay >= 0) {
            message_delay_stat_.AddSample(message_delay);
        }
        if (func_call.client_id == 0) {
            GrabFromMap(external_func_call_shm_inputs_, func_call, &input_region);
        }
        dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
        is_async_call = GrabFromMap(async_func_calls_, func_call, &async_call);
    }
    if (slog_engine_ != nullptr) {
        slog_engine_->OnFuncCallCompleted(func_call);
    }
    DCHECK(dispatcher != nullptr);
    bool ret = dispatcher->OnFuncCallFailed(func_call, message.dispatch_delay);
    if (!ret) {
        HLOG(ERROR) << "Dispatcher::OnFuncCallFailed failed";
        return;
    }
    if (func_call.client_id == 0) {
        ExternalFuncCallFailed(func_call);
    } else if (is_async_call) {
        HLOG(WARNING) << fmt::format("Async call of func {} failed",
                                     uint16_t{func_call.func_id});
    } else if (!use_fifo_for_nested_call_) {
        Message message_copy = message;
        SendFuncWorkerMessage(func_call.client_id, &message_copy);
    }
}

void Engine::OnExternalFuncCall(const FuncCall& func_call, std::span<const char> input) {
    inflight_external_requests_.fetch_add(1, std::memory_order_relaxed);
    std::unique_ptr<ipc::ShmRegion> input_region = nullptr;
    if (input.size() > MESSAGE_INLINE_DATA_SIZE) {
        input_region = ipc::ShmCreate(
            ipc::GetFuncCallInputShmName(func_call.full_call_id), input.size());
        if (input_region == nullptr) {
            ExternalFuncCallFailed(func_call);
            return;
        }
        input_region->EnableRemoveOnDestruction();
        if (input.size() > 0) {
            memcpy(input_region->base(), input.data(), input.size());
        }
    }
    Dispatcher* dispatcher = nullptr;
    mu_.AssertNotHeld();
    {
        absl::MutexLock lk(&mu_);
        incoming_external_requests_stat_.Tick();
        int64_t current_timestamp = GetMonotonicMicroTimestamp();
        if (current_timestamp <= last_external_request_timestamp_) {
            current_timestamp = last_external_request_timestamp_ + 1;
        }
        if (last_external_request_timestamp_ != -1) {
            external_requests_instant_rps_stat_.AddSample(gsl::narrow_cast<float>(
                1e6 / (current_timestamp - last_external_request_timestamp_)));
        }
        last_external_request_timestamp_ = current_timestamp;
        inflight_external_requests_stat_.AddSample(
            gsl::narrow_cast<uint16_t>(inflight_external_requests_.load(
                std::memory_order_relaxed)));
        dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
        if (input_region != nullptr) {
            if (dispatcher != nullptr) {
                external_func_call_shm_inputs_[func_call.full_call_id] = std::move(input_region);
            }
            input_use_shm_stat_.Tick();
        }
    }
    if (dispatcher == nullptr) {
        ExternalFuncCallFailed(func_call);
        return;
    }
    if (slog_engine_ != nullptr) {
        // TODO: Implement log space
        slog_engine_->OnNewExternalFuncCall(func_call, /* log_space= */ 0);
    }
    bool ret = false;
    if (input.size() <= MESSAGE_INLINE_DATA_SIZE) {
        ret = dispatcher->OnNewFuncCall(
            func_call, protocol::kInvalidFuncCall,
            input.size(), /* inline_input= */ input, /* shm_input= */ false);
    } else {
        ret = dispatcher->OnNewFuncCall(
            func_call, protocol::kInvalidFuncCall,
            input.size(), /* inline_input= */ std::span<const char>(), /* shm_input= */ true);
    }
    if (!ret) {
        HLOG(ERROR) << "Dispatcher::OnNewFuncCall failed";
        if (slog_engine_ != nullptr) {
            slog_engine_->OnFuncCallCompleted(func_call);
        }
        {
            absl::MutexLock lk(&mu_);
            GrabFromMap(external_func_call_shm_inputs_, func_call, &input_region);
        }
        ExternalFuncCallFailed(func_call);
    }
}

void Engine::HandleSharedLogOpMessage(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    if (slog_engine_ == nullptr) {
        HLOG(FATAL) << "Shared log disabled!";
        return;
    }
    slog_engine_->OnMessageFromFuncWorker(message);
}

void Engine::OnRecvMessage(MessageConnection* connection, const Message& message) {
    if (MessageHelper::IsInvokeFunc(message)) {
        HandleInvokeFuncMessage(message);
    } else if (MessageHelper::IsFuncCallComplete(message)) {
        HandleFuncCallCompleteMessage(message);
    } else if (MessageHelper::IsFuncCallFailed(message)) {
        HandleFuncCallFailedMessage(message);
    } else if (MessageHelper::IsSharedLogOp(message)) {
        HandleSharedLogOpMessage(message);
    } else {
        LOG(ERROR) << "Unknown message type!";
    }
    ProcessDiscardedFuncCallIfNecessary();
}

void Engine::SendGatewayMessage(const GatewayMessage& message, std::span<const char> payload) {
    server::IOWorker* io_worker = server::IOWorker::current();
    DCHECK(io_worker != nullptr);
    server::ConnectionBase* conn = io_worker->PickConnection(kGatewayEgressHubTypeId);
    if (conn == nullptr) {
        HLOG(ERROR) << "There is not GatewayEgressHub associated with current IOWorker";
        return;
    }
    server::EgressHub* hub = conn->as_ptr<server::EgressHub>();
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(GatewayMessage));
    hub->SendMessage(data, payload);
}

bool Engine::SendFuncWorkerMessage(uint16_t client_id, Message* message) {
    auto func_worker = worker_manager_->GetFuncWorker(client_id);
    if (func_worker == nullptr) {
        return false;
    }
    func_worker->SendMessage(message);
    return true;
}

void Engine::ExternalFuncCallCompleted(const FuncCall& func_call,
                                       std::span<const char> output, int32_t processing_time) {
    inflight_external_requests_.fetch_add(-1, std::memory_order_relaxed);
    GatewayMessage message = GatewayMessageHelper::NewFuncCallComplete(func_call, processing_time);
    message.payload_size = output.size();
    SendGatewayMessage(message, output);
}

void Engine::ExternalFuncCallFailed(const FuncCall& func_call, int status_code) {
    inflight_external_requests_.fetch_add(-1, std::memory_order_relaxed);
    GatewayMessage message = GatewayMessageHelper::NewFuncCallFailed(func_call, status_code);
    SendGatewayMessage(message);
}

Dispatcher* Engine::GetOrCreateDispatcher(uint16_t func_id) {
    absl::MutexLock lk(&mu_);
    Dispatcher* dispatcher = GetOrCreateDispatcherLocked(func_id);
    return dispatcher;
}

Dispatcher* Engine::GetOrCreateDispatcherLocked(uint16_t func_id) {
    if (dispatchers_.contains(func_id)) {
        return dispatchers_[func_id].get();
    }
    if (func_config_.find_by_func_id(func_id) != nullptr) {
        dispatchers_[func_id] = std::make_unique<Dispatcher>(this, func_id);
        return dispatchers_[func_id].get();
    } else {
        return nullptr;
    }
}

void Engine::DiscardFuncCall(const FuncCall& func_call) {
    absl::MutexLock lk(&mu_);
    discarded_func_calls_.push_back(func_call);
    discarded_func_call_stat_.Tick();
}

void Engine::ProcessDiscardedFuncCallIfNecessary() {
    std::vector<std::unique_ptr<ipc::ShmRegion>> discarded_input_regions;
    std::vector<FuncCall> discarded_external_func_calls;
    std::vector<FuncCall> discarded_internal_func_calls;
    {
        absl::MutexLock lk(&mu_);
        for (const FuncCall& func_call : discarded_func_calls_) {
            if (func_call.client_id == 0) {
                std::unique_ptr<ipc::ShmRegion> shm_input = nullptr;
                GrabFromMap(external_func_call_shm_inputs_, func_call, &shm_input);
                if (shm_input != nullptr) {
                    discarded_input_regions.push_back(std::move(shm_input));
                }
                discarded_external_func_calls.push_back(func_call);
            } else {
                discarded_internal_func_calls.push_back(func_call);
            }
        }
        discarded_func_calls_.clear();
    }
    for (const FuncCall& func_call : discarded_external_func_calls) {
        ExternalFuncCallFailed(func_call);
    }
    if (!discarded_internal_func_calls.empty()) {
        char pipe_buf[PIPE_BUF];
        Message dummy_message;
        for (const FuncCall& func_call : discarded_internal_func_calls) {
            if (use_fifo_for_nested_call_) {
                worker_lib::FifoFuncCallFinished(
                    func_call, /* success= */ false, /* output= */ std::span<const char>(),
                    /* processing_time= */ 0, pipe_buf, &dummy_message);
            } else {
                // TODO: handle this case
                NOT_IMPLEMENTED();
            }
        }
    }
}

void Engine::CreateGatewayIngressConn(int sockfd) {
    auto connection = std::make_unique<server::IngressConnection>(
        kGatewayIngressTypeId, sockfd, sizeof(GatewayMessage));
    connection->set_log_header("GatewayIngress: ");
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
        [this] (std::span<const char> data) {
            DCHECK_GE(data.size(), sizeof(GatewayMessage));
            const GatewayMessage* message = reinterpret_cast<const GatewayMessage*>(
                data.data());
            std::span<const char> payload;
            if (data.size() > sizeof(GatewayMessage)) {
                payload = data.subspan(sizeof(GatewayMessage));
            }
            OnRecvGatewayMessage(*message, payload);
        }
    );
    size_t idx = (next_gateway_conn_worker_id_++) % io_workers_.size();
    server::IOWorker* io_worker = io_workers_[idx];
    RegisterConnection(io_worker, connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!gateway_ingress_conns_.contains(connection->id()));
    gateway_ingress_conns_[connection->id()] = std::move(connection);
}

void Engine::OnNewRemoteMessageConn(int sockfd) {
    protocol::HandshakeMessage handshake;
    if (!io_utils::RecvMessage(sockfd, &handshake, nullptr)) {
        HPLOG(ERROR) << "Failed to read handshake message from engine";
        close(sockfd);
        return;
    }
    protocol::ConnType conn_type = static_cast<protocol::ConnType>(handshake.conn_type);
    switch (conn_type) {
    case protocol::ConnType::GATEWAY_TO_ENGINE:
        CreateGatewayIngressConn(sockfd);
        break;
    default:
        HLOG(ERROR) << "Invalid connection type: " << handshake.conn_type;
        close(sockfd);
        return;
    }
}

void Engine::OnNewLocalIpcConn(int sockfd) {
    std::shared_ptr<server::ConnectionBase> connection(new MessageConnection(this, sockfd));
    size_t idx = (next_ipc_conn_worker_id_++) % io_workers_.size();
    server::IOWorker* io_worker = io_workers_[idx];
    RegisterConnection(io_worker, connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!message_connections_.contains(connection->id()));
    message_connections_[connection->id()] = std::move(connection);
}

void Engine::OnNewSLogConnection(int sockfd) {
    DCHECK(slog_engine_ != nullptr);
    std::shared_ptr<server::ConnectionBase> connection(
        new IncomingSLogConnection(slog_engine_.get(), sockfd));
    size_t idx = (next_shared_log_conn_worker_id_++) % io_workers_.size();
    server::IOWorker* io_worker = io_workers_[idx];
    RegisterConnection(io_worker, connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!slog_connections_.contains(connection->id()));
    slog_connections_[connection->id()] = std::move(connection);
}

}  // namespace engine
}  // namespace faas
