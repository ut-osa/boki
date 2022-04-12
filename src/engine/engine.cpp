#include "engine/engine.h"

#include "ipc/base.h"
#include "ipc/shm_region.h"
#include "common/time.h"
#include "utils/fs.h"
#include "utils/io.h"
#include "utils/docker.h"
#include "utils/socket.h"
#include "worker/worker_lib.h"
#include "server/constants.h"
#include "log/engine.h"
#include "engine/flags.h"

#define log_header_ "Engine: "

namespace faas {
namespace engine {

using protocol::FuncCall;
using protocol::FuncCallHelper;
using protocol::Message;
using protocol::MessageHelper;
using protocol::GatewayMessage;
using protocol::GatewayMessageHelper;
using protocol::SharedLogMessage;

using server::IOWorker;
using server::ConnectionBase;
using server::IngressConnection;
using server::EgressHub;
using server::NodeWatcher;

Engine::Engine(uint16_t node_id)
    : ServerBase(fmt::format("engine_{}", node_id)),
      engine_tcp_port_(-1),
      enable_shared_log_(false),
      node_id_(node_id),
      func_worker_use_engine_socket_(absl::GetFlag(FLAGS_func_worker_use_engine_socket)),
      use_fifo_for_nested_call_(absl::GetFlag(FLAGS_use_fifo_for_nested_call)),
      ipc_sockfd_(-1),
      worker_manager_(this),
      tracer_(this),
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
      discarded_func_call_stat_(stat::Counter::StandardReportCallback("discarded_func_call")),
      next_aux_buffer_id_(0) {}

Engine::~Engine() {}

void Engine::StartInternal() {
    // Load function config file
    CHECK(!func_config_file_.empty());
    CHECK(fs_utils::ReadContents(func_config_file_, &func_config_json_))
        << "Failed to read from file " << func_config_file_;
    CHECK(func_config_.Load(func_config_json_));
    SetupLocalIpc();
    if (enable_shared_log_) {
        shared_log_engine_.reset(new log::Engine(this));
        shared_log_engine_->Start();
    }
    // Setup callbacks for node watcher
    node_watcher()->SetNodeOnlineCallback(
        absl::bind_front(&Engine::OnNodeOnline, this));
    // Initialize tracer and monitor
    tracer_.Init();
    if (absl::GetFlag(FLAGS_enable_monitor)) {
        monitor_.emplace(this);
        monitor_->Start();
    }
}

void Engine::SetupGatewayEgress() {
    bool failed = false;
    ForEachIOWorker([&failed, this] (IOWorker* io_worker) {
        EgressHub* hub = CreateEgressHub(
            protocol::ConnType::ENGINE_TO_GATEWAY, 0, io_worker);
        if (hub == nullptr) {
            HLOG(ERROR) << "Failed to create EgressHub for gateway";
            failed = true;
        }
    });
    if (failed) {
        ScheduleStop();
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
        HLOG_F(INFO, "Listen on {} for IPC connections", ipc_path);
    } else {
        std::string address = absl::GetFlag(FLAGS_listen_addr);
        CHECK(!address.empty());
        ipc_sockfd_ = utils::TcpSocketBindAndListen(
            address, gsl::narrow_cast<uint16_t>(engine_tcp_port_), listen_backlog);
        CHECK(ipc_sockfd_ != -1)
            << fmt::format("Failed to listen on {}:{}", address, engine_tcp_port_);
        HLOG_F(INFO, "Listen on {}:{} for IPC connections", address, engine_tcp_port_);
    }
    ListenForNewConnections(ipc_sockfd_, absl::bind_front(&Engine::OnNewLocalIpcConn, this));
}

void Engine::OnNodeOnline(NodeWatcher::NodeType node_type, uint16_t node_id) {
    if (node_type == NodeWatcher::kGatewayNode) {
        SetupGatewayEgress();
    }
}

void Engine::StopInternal() {
    if (ipc_sockfd_ != -1) {
        PCHECK(close(ipc_sockfd_) == 0) << "Failed to close local IPC server fd";
    }
    if (enable_shared_log_) {
        DCHECK_NOTNULL(shared_log_engine_)->Stop();
    }
}

void Engine::OnConnectionClose(ConnectionBase* connection) {
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
    case kSequencerIngressTypeId:
    case kEngineIngressTypeId:
    case kStorageIngressTypeId:
        DCHECK(ingress_conns_.contains(connection->id()));
        ingress_conns_.erase(connection->id());
        break;
    case kSequencerEgressHubTypeId:
    case kEngineEgressHubTypeId:
    case kStorageEgressHubTypeId:
        {
            absl::MutexLock lk(&conn_mu_);
            DCHECK(egress_hubs_.contains(connection->id()));
            egress_hubs_.erase(connection->id());
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
        if (monitor_.has_value() && container_id != docker_utils::kInvalidContainerId) {
            monitor_->OnNewFuncContainer(func_id, container_id);
        }
        success = worker_manager_.OnLauncherConnected(connection);
    } else {
        success = worker_manager_.OnFuncWorkerConnected(connection);
        ProcessDiscardedFuncCallIfNecessary();
    }
    if (!success) {
        return false;
    }
    if (MessageHelper::IsLauncherHandshake(handshake_message)) {
        *response = MessageHelper::NewHandshakeResponse(
            gsl::narrow_cast<uint32_t>(func_config_json_.size()));
        if (func_worker_use_engine_socket_) {
            response->flags |= protocol::kFuncWorkerUseEngineSocketFlag;
        }
        response->engine_id = node_id_;
        *response_payload = STRING_AS_SPAN(func_config_json_);
    } else {
        *response = MessageHelper::NewHandshakeResponse(0);
        if (use_fifo_for_nested_call_) {
            response->flags |= protocol::kUseFifoForNestedCallFlag;
        }
        *response_payload = EMPTY_CHAR_SPAN;
    }
    return true;
}

void Engine::OnLocalIpcConnClosed(MessageConnection* conn) {
    if (conn->handshake_done()) {
        if (conn->is_launcher_connection()) {
            worker_manager_.OnLauncherDisconnected(conn);
        } else {
            worker_manager_.OnFuncWorkerDisconnected(conn);
        }
    }
}

void Engine::OnRecvGatewayMessage(const GatewayMessage& message,
                                  std::span<const char> payload) {
    if (GatewayMessageHelper::IsDispatchFuncCall(message)) {
        FuncCall func_call = GatewayMessageHelper::GetFuncCall(message);
        OnExternalFuncCall(func_call, message.logspace, payload);
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
    if (enable_shared_log_) {
        DCHECK_NOTNULL(shared_log_engine_)->OnNewInternalFuncCall(
            func_call, parent_func_call);
    }
    bool success = false;
    if (dispatcher != nullptr) {
        if (message.payload_size < 0) {
            success = dispatcher->OnNewFuncCall(
                func_call, is_async ? protocol::kInvalidFuncCall : parent_func_call,
                /* input_size= */ gsl::narrow_cast<size_t>(-message.payload_size),
                EMPTY_CHAR_SPAN, /* shm_input= */ true);
            
        } else {
            success = dispatcher->OnNewFuncCall(
                func_call, is_async ? protocol::kInvalidFuncCall : parent_func_call,
                /* input_size= */ gsl::narrow_cast<size_t>(message.payload_size),
                MessageHelper::GetInlineData(message), /* shm_input= */ false);
        }
    }
    if (!success) {
        HLOG(ERROR) << "Dispatcher failed for func_id " << func_call.func_id;
        if (enable_shared_log_) {
            DCHECK_NOTNULL(shared_log_engine_)->OnFuncCallCompleted(func_call);
        }
        if (is_async) {
            absl::MutexLock lk(&mu_);
            async_func_calls_.erase(func_call.full_call_id);
        }
    }
    if (is_async) {
        Message response = MessageHelper::NewFuncCallComplete(
            func_call, /* processing_time= */ 0);
        SendFuncWorkerMessage(func_call.client_id, &response);
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
        if (func_call.client_id == 0) {
            // External FuncCall
            if (message.payload_size < 0) {
                output_use_shm_stat_.Tick();
            }
            GrabFromMap(external_func_call_shm_inputs_, func_call, &input_region);
        } else {
            // Internal FuncCall
            if (use_fifo_for_nested_call_) {
                DCHECK_GE(message.payload_size, 0);
                size_t msg_size = static_cast<size_t>(message.payload_size) + sizeof(int32_t);
                if (msg_size > size_t{PIPE_BUF}) {
                    output_use_shm_stat_.Tick();
                }
            } else if (message.payload_size < 0) {
                output_use_shm_stat_.Tick();
            }
        }
        dispatcher = GetOrCreateDispatcherLocked(func_call.func_id);
        is_async_call = GrabFromMap(async_func_calls_, func_call, &async_call);
    }
    if (enable_shared_log_) {
        DCHECK_NOTNULL(shared_log_engine_)->OnFuncCallCompleted(func_call);
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
            HLOG(WARNING) << "Async function call has a large output, that uses shm: "
                          << FuncCallHelper::DebugString(func_call);
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
    if (enable_shared_log_) {
        DCHECK_NOTNULL(shared_log_engine_)->OnFuncCallCompleted(func_call);
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
        HLOG_F(WARNING, "Async call of func {} failed", uint16_t{func_call.func_id});
    } else if (!use_fifo_for_nested_call_) {
        Message message_copy = message;
        SendFuncWorkerMessage(func_call.client_id, &message_copy);
    }
}

void Engine::OnExternalFuncCall(const FuncCall& func_call, uint32_t logspace,
                                std::span<const char> input) {
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
            int64_t delta = current_timestamp - last_external_request_timestamp_;
            external_requests_instant_rps_stat_.AddSample(1e6f / gsl::narrow_cast<float>(delta));
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
    if (enable_shared_log_) {
        DCHECK_NOTNULL(shared_log_engine_)->OnNewExternalFuncCall(func_call, logspace);
    }
    bool ret = false;
    if (input.size() <= MESSAGE_INLINE_DATA_SIZE) {
        ret = dispatcher->OnNewFuncCall(
            func_call, protocol::kInvalidFuncCall,
            input.size(), /* inline_input= */ input, /* shm_input= */ false);
    } else {
        ret = dispatcher->OnNewFuncCall(
            func_call, protocol::kInvalidFuncCall,
            input.size(), /* inline_input= */ EMPTY_CHAR_SPAN, /* shm_input= */ true);
    }
    if (!ret) {
        HLOG(ERROR) << "Dispatcher::OnNewFuncCall failed";
        if (enable_shared_log_) {
            DCHECK_NOTNULL(shared_log_engine_)->OnFuncCallCompleted(func_call);
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
    if (!enable_shared_log_) {
        HLOG(FATAL) << "Shared log disabled!";
        return;
    }
    DCHECK_NOTNULL(shared_log_engine_)->OnMessageFromFuncWorker(message);
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

void Engine::OnRecvAuxBuffer(MessageConnection* connection,
                             uint64_t id, std::span<const char> data) {
    VLOG_F(1, "Receive aux buffer (ID {})", bits::HexStr0x(id));
    {
        absl::MutexLock lk(&aux_buf_mu_);
        if (aux_bufs_.contains(id)) {
            LOG(FATAL) << "Duplicated aux buffer ID";
        }
        aux_bufs_[id] = std::string(data.data(), data.size());
    }
    shared_log_engine_->OnAuxBufferFromFuncWorker(id);
}

std::optional<std::string> Engine::GrabAuxBuffer(uint64_t id) {
    absl::MutexLock lk(&aux_buf_mu_);
    if (aux_bufs_.contains(id)) {
        std::string data = std::move(aux_bufs_[id]);
        aux_bufs_.erase(id);
        return data;
    } else {
        return std::nullopt;
    }
}

void Engine::SendGatewayMessage(const GatewayMessage& message, std::span<const char> payload) {
    EgressHub* hub = CurrentIOWorkerChecked()->PickConnectionAs<EgressHub>(
        kGatewayEgressHubTypeId);
    if (hub == nullptr) {
        HLOG(ERROR) << "There is not GatewayEgressHub associated with current IOWorker";
        return;
    }
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(GatewayMessage));
    hub->SendMessage(data, payload);
}

bool Engine::SendFuncWorkerMessage(uint16_t client_id, Message* message) {
    auto func_worker = worker_manager_.GetFuncWorker(client_id);
    if (func_worker == nullptr) {
        return false;
    }
    func_worker->SendMessage(message);
    return true;
}

bool Engine::SendFuncWorkerAuxBuffer(uint16_t client_id,
                                     uint64_t buf_id, std::span<const char> data) {
    auto func_worker = worker_manager_.GetFuncWorker(client_id);
    if (func_worker == nullptr) {
        return false;
    }
    func_worker->SendAuxBuffer(buf_id, data);
    return true;
}

void Engine::ExternalFuncCallCompleted(const FuncCall& func_call,
                                       std::span<const char> output, int32_t processing_time) {
    inflight_external_requests_.fetch_add(-1, std::memory_order_relaxed);
    GatewayMessage message = GatewayMessageHelper::NewFuncCallComplete(func_call, processing_time);
    message.payload_size = gsl::narrow_cast<uint32_t>(output.size());
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
                    func_call, /* success= */ false, /* output= */ EMPTY_CHAR_SPAN,
                    /* processing_time= */ 0, pipe_buf, &dummy_message);
            } else {
                // TODO: handle this case
                NOT_IMPLEMENTED();
            }
        }
    }
}

void Engine::CreateGatewayIngressConn(int sockfd) {
    auto connection = std::make_unique<IngressConnection>(
        kGatewayIngressTypeId, sockfd, sizeof(GatewayMessage));
    connection->SetMessageFullSizeCallback(
        &IngressConnection::GatewayMessageFullSizeCallback);
    connection->SetNewMessageCallback(
        IngressConnection::BuildNewGatewayMessageCallback(
            absl::bind_front(&Engine::OnRecvGatewayMessage, this)));
    RegisterConnection(PickIOWorkerForConnType(connection->type()), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!gateway_ingress_conns_.contains(connection->id()));
    gateway_ingress_conns_[connection->id()] = std::move(connection);
}

void Engine::CreateSharedLogIngressConn(int sockfd, protocol::ConnType type,
                                        uint16_t src_node_id) {
    int conn_type_id = ServerBase::GetIngressConnTypeId(type, src_node_id);
    auto connection = std::make_unique<IngressConnection>(
        conn_type_id, sockfd, sizeof(SharedLogMessage));
    connection->SetMessageFullSizeCallback(
        &IngressConnection::SharedLogMessageFullSizeCallback);
    connection->SetNewMessageCallback(
        IngressConnection::BuildNewSharedLogMessageCallback(
            absl::bind_front(&log::EngineBase::OnRecvSharedLogMessage,
                             DCHECK_NOTNULL(shared_log_engine_.get()),
                             conn_type_id & kConnectionTypeMask, src_node_id)));
    RegisterConnection(PickIOWorkerForConnType(conn_type_id), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!ingress_conns_.contains(connection->id()));
    ingress_conns_[connection->id()] = std::move(connection);
}

void Engine::OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                 int sockfd) {
    protocol::ConnType type = static_cast<protocol::ConnType>(handshake.conn_type);
    switch (type) {
    case protocol::ConnType::GATEWAY_TO_ENGINE:
        CreateGatewayIngressConn(sockfd);
        break;
    case protocol::ConnType::SEQUENCER_TO_ENGINE:
    case protocol::ConnType::STORAGE_TO_ENGINE:
    case protocol::ConnType::SLOG_ENGINE_TO_ENGINE:
        if (enable_shared_log_) {
            CreateSharedLogIngressConn(sockfd, type, handshake.src_node_id);
            break;
        }
        ABSL_FALLTHROUGH_INTENDED;
    default:
        HLOG(ERROR) << "Invalid connection type: " << handshake.conn_type;
        close(sockfd);
    }
}

void Engine::OnNewLocalIpcConn(int sockfd) {
    std::shared_ptr<ConnectionBase> connection(new MessageConnection(this, sockfd));
    RegisterConnection(PickIOWorkerForConnType(connection->type()), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!message_connections_.contains(connection->id()));
    message_connections_[connection->id()] = std::move(connection);
}

bool Engine::SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                                  const SharedLogMessage& message,
                                  std::span<const char> payload1,
                                  std::span<const char> payload2,
                                  std::span<const char> payload3) {
    DCHECK_EQ(size_t{message.payload_size},
              payload1.size() + payload2.size() + payload3.size());
    EgressHub* hub = CurrentIOWorkerChecked()->PickOrCreateConnection<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        absl::bind_front(&Engine::CreateEgressHub, this, conn_type, dst_node_id));
    if (hub == nullptr) {
        return false;
    }
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(SharedLogMessage));
    hub->SendMessage(data, payload1, payload2, payload3);
    return true;
}

EgressHub* Engine::CreateEgressHub(protocol::ConnType conn_type,
                                   uint16_t dst_node_id, IOWorker* io_worker) {
    struct sockaddr_in addr;
    if (!node_watcher()->GetNodeAddr(NodeWatcher::GetDstNodeType(conn_type),
                                     dst_node_id, &addr)) {
        return nullptr;
    }
    auto egress_hub = std::make_unique<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        &addr, absl::GetFlag(FLAGS_message_conn_per_worker));
    uint16_t src_node_id = node_id_;
    egress_hub->SetHandshakeMessageCallback(
        [conn_type, src_node_id] (std::string* handshake) {
            *handshake = protocol::EncodeHandshakeMessage(conn_type, src_node_id);
        }
    );
    RegisterConnection(io_worker, egress_hub.get());
    DCHECK_GE(egress_hub->id(), 0);
    EgressHub* hub = egress_hub.get();
    {
        absl::MutexLock lk(&conn_mu_);
        DCHECK(!egress_hubs_.contains(egress_hub->id()));
        egress_hubs_[egress_hub->id()] = std::move(egress_hub);
    }
    return hub;
}

}  // namespace engine
}  // namespace faas
