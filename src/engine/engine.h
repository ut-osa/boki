#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "common/sequencer_config.h"
#include "ipc/shm_region.h"
#include "server/server_base.h"
#include "server/ingress_connection.h"
#include "server/egress_hub.h"
#include "engine/message_connection.h"
#include "engine/dispatcher.h"
#include "engine/worker_manager.h"
#include "engine/monitor.h"
#include "engine/tracer.h"
#include "engine/timer.h"
#include "engine/slog_connection.h"
#include "engine/slog_engine.h"

namespace faas {
namespace engine {

class Engine final : public server::ServerBase {
public:
    Engine();
    ~Engine();

    void set_node_id(uint16_t value) { node_id_ = value; }
    void set_func_config_file(std::string_view path) {
        func_config_file_ = std::string(path);
    }
    void set_sequencer_config_file(std::string_view path) {
        sequencer_config_file_ = std::string(path);
    }
    void enable_shared_log() { enable_shared_log_ = true; }
    void set_engine_tcp_port(int port) { engine_tcp_port_ = port; }
    void set_shared_log_tcp_port(int port) { shared_log_tcp_port_ = port; }

    uint16_t node_id() const { return node_id_; }
    const FuncConfig* func_config() { return &func_config_; }
    int engine_tcp_port() const { return engine_tcp_port_; }
    int shared_log_tcp_port() const { return shared_log_tcp_port_; }
    bool func_worker_use_engine_socket() const { return func_worker_use_engine_socket_; }
    WorkerManager* worker_manager() { return worker_manager_.get(); }
    Monitor* monitor() { return monitor_.get(); }
    Tracer* tracer() { return tracer_.get(); }

    // Must be thread-safe
    bool OnNewHandshake(MessageConnection* connection,
                        const protocol::Message& handshake_message,
                        protocol::Message* response,
                        std::span<const char>* response_payload);
    void OnRecvMessage(MessageConnection* connection, const protocol::Message& message);
    Dispatcher* GetOrCreateDispatcher(uint16_t func_id);
    void DiscardFuncCall(const protocol::FuncCall& func_call);

private:
    class ExternalFuncCallContext;
    friend class SLogEngine;

    int engine_tcp_port_;
    bool enable_shared_log_;
    int shared_log_tcp_port_;
    uint16_t node_id_;
    std::string func_config_file_;
    std::string func_config_json_;
    FuncConfig func_config_;
    std::string sequencer_config_file_;
    SequencerConfig sequencer_config_;
    bool func_worker_use_engine_socket_;
    bool use_fifo_for_nested_call_;

    int message_sockfd_;
    int ipc_sockfd_;
    int shared_log_sockfd_;

    std::vector<server::IOWorker*> io_workers_;
    size_t next_gateway_conn_worker_id_;
    size_t next_ipc_conn_worker_id_;
    size_t next_shared_log_conn_worker_id_;

    absl::flat_hash_map</* id */ int, std::unique_ptr<server::EgressHub>>
        gateway_egress_hubs_;
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        gateway_ingress_conns_;

    absl::flat_hash_map</* id */ int, std::shared_ptr<server::ConnectionBase>> message_connections_;
    absl::flat_hash_map</* id */ int, std::shared_ptr<server::ConnectionBase>> sequencer_connections_;
    absl::flat_hash_map</* id */ int, std::shared_ptr<server::ConnectionBase>> slog_connections_;
    absl::flat_hash_set<std::unique_ptr<SLogMessageHub>> slog_message_hubs_;
    absl::flat_hash_set<std::unique_ptr<Timer>> timers_;

    std::unique_ptr<WorkerManager> worker_manager_;
    std::unique_ptr<Monitor> monitor_;
    std::unique_ptr<Tracer> tracer_;
    std::unique_ptr<SLogEngine> slog_engine_;

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

    int64_t last_external_request_timestamp_ ABSL_GUARDED_BY(mu_);
    stat::Counter incoming_external_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter incoming_internal_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<float> external_requests_instant_rps_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> inflight_external_requests_stat_ ABSL_GUARDED_BY(mu_);

    stat::StatisticsCollector<int32_t> message_delay_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter input_use_shm_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter output_use_shm_stat_ ABSL_GUARDED_BY(mu_);
    stat::Counter discarded_func_call_stat_ ABSL_GUARDED_BY(mu_);

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;

    void SetupGatewayEgress();
    void SetupLocalIpc();
    void SetupSharedLog();
    void SetupMessageServer();

    Timer* CreateTimer(int timer_type, server::IOWorker* io_worker, Timer::Callback cb);
    Timer* CreatePeriodicTimer(int timer_type, server::IOWorker* io_worker,
                               absl::Time initial, absl::Duration duration,
                               Timer::Callback cb);

    void CreateGatewayIngressConn(int sockfd);
    void OnNewRemoteMessageConn(int sockfd);

    void OnNewLocalIpcConn(int sockfd);
    void OnLocalIpcConnClosed(MessageConnection* conn);

    void OnNewSLogConnection(int sockfd);

    // Must be thread-safe
    void HandleInvokeFuncMessage(const protocol::Message& message);
    void HandleFuncCallCompleteMessage(const protocol::Message& message);
    void HandleFuncCallFailedMessage(const protocol::Message& message);
    void HandleSharedLogOpMessage(const protocol::Message& message);

    void OnRecvGatewayMessage(const protocol::GatewayMessage& message,
                              std::span<const char> payload);
    void SendGatewayMessage(const protocol::GatewayMessage& message,
                            std::span<const char> payload = std::span<const char>());
    bool SendFuncWorkerMessage(uint16_t client_id, protocol::Message* message);
    void OnExternalFuncCall(const protocol::FuncCall& func_call, std::span<const char> input);
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
