#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/flags.h"
#include "common/func_config.h"
#include "ipc/shm_region.h"
#include "engine/server_base.h"
#include "engine/gateway_connection.h"
#include "engine/message_connection.h"
#include "engine/dispatcher.h"
#include "engine/worker_manager.h"
#include "engine/monitor.h"
#include "engine/tracer.h"
#include "engine/shared_log_connection.h"
#include "engine/shared_log_engine.h"

namespace faas {
namespace engine {

class Engine final : public ServerBase {
public:
    static constexpr int kDefaultListenBackLog = 64;
    static constexpr int kDefaultNumIOWorkers = 1;
    static constexpr int kDefaultGatewayConnPerWorker = 2;
    static constexpr int kDefaultEngineConnPerWorker = 2;

    Engine();
    ~Engine();

    void set_gateway_addr_port(std::string_view addr, int port) {
        gateway_addr_ = std::string(addr);
        gateway_port_ = port;
    }
    void set_num_io_workers(int value) { num_io_workers_ = value; }
    void set_gateway_conn_per_worker(int value) { gateway_conn_per_worker_ = value; }
    void set_engine_conn_per_worker(int value) { engine_conn_per_worker_ = value; }
    void set_node_id(uint16_t value) { node_id_ = value; }
    void set_func_config_file(std::string_view path) {
        func_config_file_ = std::string(path);
    }
    void set_engine_tcp_port(int port) { engine_tcp_port_ = port; }
    void set_shared_log_tcp_host(std::string_view host) {
        shared_log_tcp_host_ = std::string(host);
    }
    void set_shared_log_tcp_port(int port) { shared_log_tcp_port_ = port; }

    uint16_t node_id() const { return node_id_; }
    FuncConfig* func_config() { return &func_config_; }
    int engine_tcp_port() const { return engine_tcp_port_; }
    std::string_view shared_log_tcp_host() const { return shared_log_tcp_host_; }
    int shared_log_tcp_port() const { return shared_log_tcp_port_; }
    bool func_worker_use_engine_socket() const { return func_worker_use_engine_socket_; }
    int engine_conn_per_worker() const { return engine_conn_per_worker_; }
    WorkerManager* worker_manager() { return worker_manager_.get(); }
    Monitor* monitor() { return monitor_.get(); }
    Tracer* tracer() { return tracer_.get(); }

    // Must be thread-safe
    bool OnNewHandshake(MessageConnection* connection,
                        const protocol::Message& handshake_message,
                        protocol::Message* response,
                        std::span<const char>* response_payload);
    void OnRecvMessage(MessageConnection* connection, const protocol::Message& message);
    void OnRecvSharedLogMessage(const protocol::Message& message);
    void OnRecvGatewayMessage(GatewayConnection* connection,
                              const protocol::GatewayMessage& message,
                              std::span<const char> payload);
    Dispatcher* GetOrCreateDispatcher(uint16_t func_id);
    void DiscardFuncCall(const protocol::FuncCall& func_call);

private:
    class ExternalFuncCallContext;
    friend class SharedLogEngine;

    std::string gateway_addr_;
    int gateway_port_;
    int listen_backlog_;
    int num_io_workers_;
    int gateway_conn_per_worker_;
    int engine_conn_per_worker_;
    int engine_tcp_port_;
    std::string shared_log_tcp_host_;
    int shared_log_tcp_port_;
    uint16_t node_id_;
    std::string func_config_file_;
    std::string func_config_json_;
    FuncConfig func_config_;
    bool func_worker_use_engine_socket_;
    bool use_fifo_for_nested_call_;

    int server_sockfd_;
    int shared_log_sockfd_;

    std::vector<IOWorker*> io_workers_;
    size_t next_ipc_conn_worker_id_;
    size_t next_shared_log_conn_worker_id_;

    absl::flat_hash_map</* id */ int, std::shared_ptr<ConnectionBase>> message_connections_;
    absl::flat_hash_map</* id */ int, std::shared_ptr<ConnectionBase>> gateway_connections_;
    absl::flat_hash_map</* id */ int, std::shared_ptr<ConnectionBase>> shared_log_connections_;
    absl::flat_hash_set<std::unique_ptr<SharedLogMessageHub>> shared_log_message_hubs_;

    std::unique_ptr<WorkerManager> worker_manager_;
    std::unique_ptr<Monitor> monitor_;
    std::unique_ptr<Tracer> tracer_;
    std::unique_ptr<SharedLogEngine> shared_log_engine_;

    std::atomic<int> inflight_external_requests_;

    absl::Mutex mu_;
    absl::flat_hash_map</* full_call_id */ uint64_t, std::unique_ptr<ipc::ShmRegion>>
        external_func_call_shm_inputs_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* func_id */ uint16_t, std::unique_ptr<Dispatcher>>
        dispatchers_ ABSL_GUARDED_BY(mu_);
    std::vector<protocol::FuncCall> discarded_func_calls_ ABSL_GUARDED_BY(mu_);

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
    void OnConnectionClose(ConnectionBase* connection) override;

    void OnNewMessageConnection(int sockfd);
    void OnNewSharedLogConnection(int sockfd);

    // Must be thread-safe
    void HandleInvokeFuncMessage(const protocol::Message& message);
    void HandleFuncCallCompleteMessage(const protocol::Message& message);
    void HandleFuncCallFailedMessage(const protocol::Message& message);
    void HandleSharedLogOpMessage(const protocol::Message& message);

    void SendGatewayMessage(const protocol::GatewayMessage& message,
                            std::span<const char> payload = std::span<const char>());
    void OnExternalFuncCall(const protocol::FuncCall& func_call, std::span<const char> input);
    void ExternalFuncCallCompleted(const protocol::FuncCall& func_call,
                                   std::span<const char> output, int32_t processing_time);
    void ExternalFuncCallFailed(const protocol::FuncCall& func_call, int status_code = 0);

    Dispatcher* GetOrCreateDispatcherLocked(uint16_t func_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    std::unique_ptr<ipc::ShmRegion> GrabExternalFuncCallShmInput(
            const protocol::FuncCall& func_call) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void ProcessDiscardedFuncCallIfNecessary();

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

}  // namespace engine
}  // namespace faas
