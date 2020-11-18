#include "engine/engine.h"

#include "ipc/base.h"
#include "ipc/shm_region.h"
#include "common/time.h"
#include "utils/fs.h"
#include "utils/io.h"
#include "utils/docker.h"
#include "utils/socket.h"
#include "worker/worker_lib.h"
#include "engine/flags.h"
#include "engine/constants.h"
#include "engine/sequencer_connection.h"

#define HLOG(l) LOG(l) << "Engine: "
#define HVLOG(l) VLOG(l) << "Engine: "

namespace faas {
namespace engine {

using protocol::FuncCall;
using protocol::Message;
using protocol::MessageHelper;
using protocol::GatewayMessage;
using protocol::GatewayMessageHelper;
using protocol::SharedLogOpType;

Engine::Engine()
    : gateway_port_(-1),
      num_io_workers_(kDefaultNumIOWorkers),
      engine_tcp_port_(-1),
      enable_shared_log_(false),
      sequencer_port_(-1),
      shared_log_tcp_port_(-1),
      func_worker_use_engine_socket_(absl::GetFlag(FLAGS_func_worker_use_engine_socket)),
      use_fifo_for_nested_call_(absl::GetFlag(FLAGS_use_fifo_for_nested_call)),
      server_sockfd_(-1),
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
      discarded_func_call_stat_(stat::Counter::StandardReportCallback("discarded_func_call")) {
}

Engine::~Engine() {}

void Engine::StartInternal() {
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
    SetupGatewayConnections();
    SetupLocalIpc();
    if (enable_shared_log_) {
        SetupSharedLog();
    }
    // Initialize tracer
    tracer_->Init();
}

void Engine::SetupGatewayConnections() {
    CHECK(!gateway_addr_.empty());
    CHECK_NE(gateway_port_, -1);
    int total_gateway_conn = num_io_workers_ * absl::GetFlag(FLAGS_gateway_conn_per_worker);
    for (int i = 0; i < total_gateway_conn; i++) {
        int sockfd = utils::TcpSocketConnect(gateway_addr_, gateway_port_);
        CHECK(sockfd != -1)
            << fmt::format("Failed to connect to gateway {}:{}", gateway_addr_, gateway_port_);
        std::shared_ptr<ConnectionBase> connection(
            new GatewayConnection(this, gsl::narrow_cast<uint16_t>(i), sockfd));
        IOWorker* io_worker = io_workers_[i % num_io_workers_];
        RegisterConnection(io_worker, connection.get());
        DCHECK_GE(connection->id(), 0);
        DCHECK(!gateway_connections_.contains(connection->id()));
        gateway_connections_[connection->id()] = std::move(connection);
    }
}

void Engine::SetupLocalIpc() {
    int listen_backlog = absl::GetFlag(FLAGS_socket_listen_backlog);
    if (engine_tcp_port_ == -1) {
        std::string ipc_path(ipc::GetEngineUnixSocketPath());
        if (fs_utils::Exists(ipc_path)) {
            PCHECK(fs_utils::Remove(ipc_path));
        }
        server_sockfd_ = utils::UnixDomainSocketBindAndListen(ipc_path, listen_backlog);
        CHECK(server_sockfd_ != -1)
            << fmt::format("Failed to listen on {}", ipc_path);
        HLOG(INFO) << fmt::format("Listen on {} for IPC connections", ipc_path);
    } else {
        server_sockfd_ = utils::TcpSocketBindAndListen(
            "0.0.0.0", engine_tcp_port_, listen_backlog);
        CHECK(server_sockfd_ != -1)
            << fmt::format("Failed to listen on 0.0.0.0:{}", engine_tcp_port_);
        HLOG(INFO) << fmt::format("Listen on 0.0.0.0:{} for IPC connections", engine_tcp_port_);
    }
    ListenForNewConnections(server_sockfd_,
                            absl::bind_front(&Engine::OnNewMessageConnection, this));
}

void Engine::SetupSharedLog() {
    DCHECK(enable_shared_log_);
    slog_engine_.reset(new SLogEngine(this));
    // Connect to sequencer
    CHECK(!sequencer_addr_.empty());
    CHECK_NE(sequencer_port_, -1);
    int total_sequencer_conn = num_io_workers_ * absl::GetFlag(FLAGS_sequencer_conn_per_worker);
    for (int i = 0; i < total_sequencer_conn; i++) {
        int sockfd = utils::TcpSocketConnect(sequencer_addr_, sequencer_port_);
        CHECK(sockfd != -1)
            << fmt::format("Failed to connect to sequencer {}:{}",
                           sequencer_addr_, gateway_port_);
        std::shared_ptr<ConnectionBase> connection(
            new SequencerConnection(this, slog_engine_.get(), sockfd));
        IOWorker* io_worker = io_workers_[i % num_io_workers_];
        RegisterConnection(io_worker, connection.get());
        DCHECK_GE(connection->id(), 0);
        DCHECK(!sequencer_connections_.contains(connection->id()));
        sequencer_connections_[connection->id()] = std::move(connection);
    }
    // Setup SharedLogMessageHub for each IOWorker
    for (size_t i = 0; i < io_workers_.size(); i++) {
        auto hub = std::make_unique<SLogMessageHub>(slog_engine_.get());
        RegisterConnection(io_workers_[i], hub.get());
        slog_message_hubs_.insert(std::move(hub));
    }
    // Listen on shared_log_tcp_port
    CHECK_NE(shared_log_tcp_port_, -1);
    shared_log_sockfd_ = utils::TcpSocketBindAndListen(
        "0.0.0.0", shared_log_tcp_port_, absl::GetFlag(FLAGS_socket_listen_backlog));
    CHECK(shared_log_sockfd_ != -1)
        << fmt::format("Failed to listen on 0.0.0.0:{}", shared_log_tcp_port_);
    HLOG(INFO) << fmt::format("Listen on 0.0.0.0:{} for shared log related connections",
                              shared_log_tcp_port_);
    ListenForNewConnections(shared_log_sockfd_,
                            absl::bind_front(&Engine::OnNewSLogConnection, this));
}

void Engine::StopInternal() {
    if (server_sockfd_ != -1) {
        PCHECK(close(server_sockfd_) == 0) << "Failed to close server fd";
    }
    if (shared_log_sockfd_ != -1) {
        PCHECK(close(shared_log_sockfd_) == 0) << "Failed to close server fd";
    }
}

Timer* Engine::CreateTimer(int timer_type, IOWorker* io_worker, Timer::Callback cb) {
    Timer* timer = new Timer(timer_type, cb);
    RegisterConnection(io_worker, timer);
    timers_.insert(std::unique_ptr<Timer>(timer));
    return timer;
}

void Engine::OnConnectionClose(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    if (connection->type() == kMessageConnectionTypeId) {
        DCHECK(message_connections_.contains(connection->id()));
        MessageConnection* message_connection = connection->as_ptr<MessageConnection>();
        if (message_connection->handshake_done()) {
            if (message_connection->is_launcher_connection()) {
                worker_manager_->OnLauncherDisconnected(message_connection);
            } else {
                worker_manager_->OnFuncWorkerDisconnected(message_connection);
            }
        }
        message_connections_.erase(connection->id());
        HLOG(INFO) << "A MessageConnection is returned";
    } else if (connection->type() == kGatewayConnectionTypeId) {
        DCHECK(gateway_connections_.contains(connection->id()));
        GatewayConnection* gateway_connection = connection->as_ptr<GatewayConnection>();
        HLOG(WARNING) << fmt::format("Gateway connection (conn_id={}) disconencted",
                                     gateway_connection->conn_id());
        gateway_connections_.erase(connection->id());
    } else if (connection->type() == kSequencerConnectionTypeId) {
        DCHECK(sequencer_connections_.contains(connection->id()));
        HLOG(WARNING) << "Sequencer connection disconencted";
        sequencer_connections_.erase(connection->id());
    } else if (connection->type() == kIncomingSLogConnectionTypeId) {
        DCHECK(slog_connections_.contains(connection->id()));
        slog_connections_.erase(connection->id());
    } else if (connection->type() == kSLogMessageHubTypeId) {
        if (state_.load() != kStopping) {
            HLOG(FATAL) << "SLogMessageHub should not be closed";
        }
    } else if (timers_.contains(connection->as_ptr<Timer>())) {
        timers_.erase(connection->as_ptr<Timer>());
    } else {
        HLOG(FATAL) << "Unknown connection type!";
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
        *response_payload = std::span<const char>(func_config_json_.data(),
                                                  func_config_json_.size());
    } else {
        *response = MessageHelper::NewHandshakeResponse(0);
        if (use_fifo_for_nested_call_) {
            response->flags |= protocol::kUseFifoForNestedCallFlag;
        }
        *response_payload = std::span<const char>();
    }
    return true;
}

void Engine::OnRecvGatewayMessage(GatewayConnection* connection, const GatewayMessage& message,
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
    }
    bool success = false;
    if (dispatcher != nullptr) {
        if (message.payload_size < 0) {
            success = dispatcher->OnNewFuncCall(
                func_call, parent_func_call,
                /* input_size= */ gsl::narrow_cast<size_t>(-message.payload_size),
                std::span<const char>(), /* shm_input= */ true);
            
        } else {
            success = dispatcher->OnNewFuncCall(
                func_call, parent_func_call,
                /* input_size= */ gsl::narrow_cast<size_t>(message.payload_size),
                MessageHelper::GetInlineData(message), /* shm_input= */ false);
        }
    }
    if (!success) {
        HLOG(ERROR) << "Dispatcher failed for func_id " << func_call.func_id;
    }
}

void Engine::HandleFuncCallCompleteMessage(const Message& message) {
    DCHECK(MessageHelper::IsFuncCallComplete(message));
    int32_t message_delay = MessageHelper::ComputeMessageDelay(message);
    FuncCall func_call = MessageHelper::GetFuncCall(message);
    Dispatcher* dispatcher = nullptr;
    std::unique_ptr<ipc::ShmRegion> input_region = nullptr;
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
            input_region = GrabExternalFuncCallShmInput(func_call);
        }
        dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
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
    mu_.AssertNotHeld();
    {
        absl::MutexLock lk(&mu_);
        if (message_delay >= 0) {
            message_delay_stat_.AddSample(message_delay);
        }
        if (func_call.client_id == 0) {
            input_region = GrabExternalFuncCallShmInput(func_call);
        }
        dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
    }
    DCHECK(dispatcher != nullptr);
    bool ret = dispatcher->OnFuncCallFailed(func_call, message.dispatch_delay);
    if (!ret) {
        HLOG(ERROR) << "Dispatcher::OnFuncCallFailed failed";
        return;
    }
    if (func_call.client_id == 0) {
        ExternalFuncCallFailed(func_call);
    } else if (!use_fifo_for_nested_call_) {
        Message message_copy = message;
        SendFuncWorkerMessage(func_call.client_id, &message_copy);
    }
}

void Engine::OnExternalFuncCall(const FuncCall& func_call, std::span<const char> input) {
    inflight_external_requests_.fetch_add(1);
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
            gsl::narrow_cast<uint16_t>(inflight_external_requests_.load()));
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
        {
            absl::MutexLock lk(&mu_);
            input_region = GrabExternalFuncCallShmInput(func_call);
        }
        ExternalFuncCallFailed(func_call);
    }
}

void Engine::HandleSharedLogOpMessage(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    if (slog_engine_ == nullptr) {
        HLOG(ERROR) << "Shared log disabled!";
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
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* conn = io_worker->PickConnection(kGatewayConnectionTypeId);
    if (conn == nullptr) {
        HLOG(ERROR) << "There is not GatewayConnection associated with current IOWorker";
        return;
    }
    conn->as_ptr<GatewayConnection>()->SendMessage(message, payload);
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
    inflight_external_requests_.fetch_add(-1);
    GatewayMessage message = GatewayMessageHelper::NewFuncCallComplete(func_call, processing_time);
    message.payload_size = output.size();
    SendGatewayMessage(message, output);
}

void Engine::ExternalFuncCallFailed(const FuncCall& func_call, int status_code) {
    inflight_external_requests_.fetch_add(-1);
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

std::unique_ptr<ipc::ShmRegion> Engine::GrabExternalFuncCallShmInput(const FuncCall& func_call) {
    std::unique_ptr<ipc::ShmRegion> ret = nullptr;
    if (external_func_call_shm_inputs_.contains(func_call.full_call_id)) {
        ret = std::move(external_func_call_shm_inputs_[func_call.full_call_id]);
        external_func_call_shm_inputs_.erase(func_call.full_call_id);
    }
    return ret;
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
                auto shm_input = GrabExternalFuncCallShmInput(func_call);
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
                LOG(FATAL) << "Unimplemented";
            }
        }
    }
}

void Engine::OnNewMessageConnection(int sockfd) {
    HLOG(INFO) << "New message connection";
    std::shared_ptr<ConnectionBase> connection(new MessageConnection(this, sockfd));
    DCHECK_LT(next_ipc_conn_worker_id_, io_workers_.size());
    IOWorker* io_worker = io_workers_[next_ipc_conn_worker_id_];
    next_ipc_conn_worker_id_ = (next_ipc_conn_worker_id_ + 1) % io_workers_.size();
    RegisterConnection(io_worker, connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!message_connections_.contains(connection->id()));
    message_connections_[connection->id()] = std::move(connection);
}

void Engine::OnNewSLogConnection(int sockfd) {
    HLOG(INFO) << "New shared log connection";
    DCHECK(slog_engine_ != nullptr);
    std::shared_ptr<ConnectionBase> connection(
        new IncomingSLogConnection(slog_engine_.get(), sockfd));
    DCHECK_LT(next_shared_log_conn_worker_id_, io_workers_.size());
    IOWorker* io_worker = io_workers_[next_shared_log_conn_worker_id_];
    next_shared_log_conn_worker_id_ = (next_shared_log_conn_worker_id_ + 1) % io_workers_.size();
    RegisterConnection(io_worker, connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!slog_connections_.contains(connection->id()));
    slog_connections_[connection->id()] = std::move(connection);
}

}  // namespace engine
}  // namespace faas
