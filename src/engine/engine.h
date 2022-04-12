#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "ipc/shm_region.h"
#include "server/server_base.h"
#include "server/ingress_connection.h"
#include "server/egress_hub.h"
#include "engine/message_connection.h"
#include "engine/dispatcher.h"
#include "engine/worker_manager.h"
#include "engine/monitor.h"
#include "engine/tracer.h"

namespace faas {

// Forward declaration
namespace log { class EngineBase; }

namespace engine {

class Engine final : public server::ServerBase {
public:
    explicit Engine(uint16_t node_id);
    ~Engine();

    void set_func_config_file(std::string_view path) {
        func_config_file_ = std::string(path);
    }
    void enable_shared_log() { enable_shared_log_ = true; }
    void set_engine_tcp_port(int port) { engine_tcp_port_ = port; }

    uint16_t node_id() const { return node_id_; }
    const FuncConfig* func_config() { return &func_config_; }
    int engine_tcp_port() const { return engine_tcp_port_; }
    bool func_worker_use_engine_socket() const { return func_worker_use_engine_socket_; }
    WorkerManager* worker_manager() { return &worker_manager_; }
    Monitor* monitor() { return &monitor_.value(); }
    Tracer* tracer() { return &tracer_; }

    // Must be thread-safe
    bool OnNewHandshake(MessageConnection* connection,
                        const protocol::Message& handshake_message,
                        protocol::Message* response,
                        std::span<const char>* response_payload);
    void OnRecvMessage(MessageConnection* connection, const protocol::Message& message);
    Dispatcher* GetOrCreateDispatcher(uint16_t func_id);
    void DiscardFuncCall(const protocol::FuncCall& func_call);

    void OnRecvAuxBuffer(MessageConnection* connection,
                         uint64_t id, std::span<const char> data);
    std::optional<std::string> GrabAuxBuffer(uint64_t id);

private:
    class ExternalFuncCallContext;
    friend class log::EngineBase;

    int engine_tcp_port_;
    bool enable_shared_log_;
    uint16_t node_id_;
    std::string func_config_file_;
    std::string func_config_json_;
    FuncConfig func_config_;
    bool func_worker_use_engine_socket_;
    bool use_fifo_for_nested_call_;

    int ipc_sockfd_;

    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        gateway_ingress_conns_;
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        ingress_conns_;

    absl::Mutex conn_mu_;
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::EgressHub>>
        egress_hubs_ ABSL_GUARDED_BY(conn_mu_);

    absl::flat_hash_map</* id */ int,
                        std::shared_ptr<server::ConnectionBase>> message_connections_;

    WorkerManager worker_manager_;
    std::optional<Monitor> monitor_;
    Tracer tracer_;
    std::unique_ptr<log::EngineBase> shared_log_engine_;

    std::atomic<int> inflight_external_requests_;

    absl::Mutex mu_;
    absl::flat_hash_map</* full_call_id */ uint64_t, std::unique_ptr<ipc::ShmRegion>>
        external_func_call_shm_inputs_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* func_id */ uint16_t, std::unique_ptr<Dispatcher>>
        dispatchers_ ABSL_GUARDED_BY(mu_);
    std::vector<protocol::FuncCall> discarded_func_calls_ ABSL_GUARDED_BY(mu_);

    struct AsyncFuncCall {
        protocol::FuncCall func_call;
        protocol::FuncCall parent_func_call;
        std::unique_ptr<ipc::ShmRegion> input_region;
    };
    absl::flat_hash_map</* full_call_id */ uint64_t, AsyncFuncCall>
        async_func_calls_ ABSL_GUARDED_BY(mu_);
    
    absl::Mutex aux_buf_mu_;
    absl::flat_hash_map</* id */ uint64_t, std::string>
        aux_bufs_ ABSL_GUARDED_BY(aux_buf_mu_);

    int64_t last_external_request_timestamp_ ABSL_GUARDED_BY(mu_);
    stat::Counter incoming_external_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter incoming_internal_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<float> external_requests_instant_rps_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> inflight_external_requests_stat_ ABSL_GUARDED_BY(mu_);

    stat::StatisticsCollector<int32_t> message_delay_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter input_use_shm_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter output_use_shm_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter discarded_func_call_stat_ ABSL_GUARDED_BY(mu_);

    std::atomic<uint64_t> next_aux_buffer_id_;

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;
    void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                             int sockfd) override;

    void SetupGatewayEgress();
    void SetupLocalIpc();

    void OnNodeOnline(server::NodeWatcher::NodeType node_type, uint16_t node_id);

    void CreateGatewayIngressConn(int sockfd);
    void CreateSharedLogIngressConn(int sockfd, protocol::ConnType type,
                                    uint16_t src_node_id);

    void OnNewLocalIpcConn(int sockfd);
    void OnLocalIpcConnClosed(MessageConnection* conn);

    bool SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                              const protocol::SharedLogMessage& message,
                              std::span<const char> payload1 = EMPTY_CHAR_SPAN,
                              std::span<const char> payload2 = EMPTY_CHAR_SPAN,
                              std::span<const char> payload3 = EMPTY_CHAR_SPAN);
    server::EgressHub* CreateEgressHub(protocol::ConnType conn_type,
                                       uint16_t dst_node_id,
                                       server::IOWorker* io_worker);

    // Must be thread-safe
    void HandleInvokeFuncMessage(const protocol::Message& message);
    void HandleFuncCallCompleteMessage(const protocol::Message& message);
    void HandleFuncCallFailedMessage(const protocol::Message& message);
    void HandleSharedLogOpMessage(const protocol::Message& message);

    void OnRecvGatewayMessage(const protocol::GatewayMessage& message,
                              std::span<const char> payload);
    void SendGatewayMessage(const protocol::GatewayMessage& message,
                            std::span<const char> payload = EMPTY_CHAR_SPAN);
    bool SendFuncWorkerMessage(uint16_t client_id, protocol::Message* message);
    bool SendFuncWorkerAuxBuffer(uint16_t client_id,
                                 uint64_t buf_id, std::span<const char> data);
    void OnExternalFuncCall(const protocol::FuncCall& func_call, uint32_t logspace,
                            std::span<const char> input);
    void ExternalFuncCallCompleted(const protocol::FuncCall& func_call,
                                   std::span<const char> output, int32_t processing_time);
    void ExternalFuncCallFailed(const protocol::FuncCall& func_call, int status_code = 0);
    void AsyncFuncCallFinished(AsyncFuncCall async_call, bool success,
                               bool shm_output, std::span<const char> inline_output);

    Dispatcher* GetOrCreateDispatcherLocked(uint16_t func_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void ProcessDiscardedFuncCallIfNecessary();

    template<class ValueT>
    bool GrabFromMap(absl::flat_hash_map<uint64_t, ValueT>& map,
                     const protocol::FuncCall& func_call, ValueT* value);

    uint64_t NextAuxBufferId() {
        return next_aux_buffer_id_.fetch_add(1, std::memory_order_relaxed);
    }

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

template<class ValueT>
bool Engine::GrabFromMap(absl::flat_hash_map<uint64_t, ValueT>& map,
                         const protocol::FuncCall& func_call, ValueT* value) {
    if (!map.contains(func_call.full_call_id)) {
        return false;
    }
    *value = std::move(map[func_call.full_call_id]);
    map.erase(func_call.full_call_id);
    return true;
}

}  // namespace engine
}  // namespace faas
