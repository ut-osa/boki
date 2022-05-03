#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/zk.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "utils/blocking_queue.h"
#include "server/server_base.h"
#include "server/ingress_connection.h"
#include "server/egress_hub.h"
#include "gateway/func_call_context.h"
#include "gateway/http_connection.h"
#include "gateway/grpc_connection.h"
#include "gateway/node_manager.h"

namespace faas {
namespace gateway {

class Server final : public server::ServerBase {
public:
    Server();
    ~Server();

    void set_http_port(int port) { http_port_ = port; }
    void set_grpc_port(int port) { grpc_port_ = port; }
    void set_func_config_file(std::string_view path) {
        func_config_file_ = std::string(path);
    }
    FuncConfig* func_config() { return &func_config_; }
    NodeManager* node_manager() { return &node_manager_; }

    // Must be thread-safe
    void OnEngineNodeOnline(uint16_t node_id);
    void OnEngineNodeOffline(uint16_t node_id);
    void OnNewHttpFuncCall(HttpConnection* connection, FuncCallContext* func_call_context);
    void OnNewGrpcFuncCall(GrpcConnection* connection, FuncCallContext* func_call_context);
    void DiscardFuncCall(FuncCallContext* func_call_context);
    void OnRecvEngineMessage(uint16_t node_id, const protocol::GatewayMessage& message,
                             std::span<const char> payload);

private:
    int http_port_;
    int grpc_port_;
    std::string func_config_file_;
    FuncConfig func_config_;

    int http_sockfd_;
    int grpc_sockfd_;
    std::vector<server::IOWorker*> io_workers_;

    int next_http_connection_id_;
    int next_grpc_connection_id_;

    NodeManager node_manager_;
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        engine_ingress_conns_;

    std::atomic<uint32_t> next_call_id_;

    struct FuncCallState {
        protocol::FuncCall func_call;
        uint32_t           logspace;
        std::set<uint16_t> node_constraint;
        int                connection_id;  // of HttpConnection or GrpcConnection, or -1 for async calls
        FuncCallContext*   context;
        int64_t            recv_timestamp;
        int64_t            dispatch_timestamp;
        // Will only be used for async call
        std::string        input;
    };

    struct AsyncCallResult {
        bool        success;
        uint16_t    func_id;
        uint32_t    logspace;
        int64_t     recv_timestamp;
        int64_t     dispatch_timestamp;
        int64_t     finished_timestamp;
        std::string output;
    };

    utils::BlockingQueue<AsyncCallResult> async_call_results_;
    base::Thread background_thread_;

    struct PerFuncStat {
        int64_t last_request_timestamp;
        stat::Counter incoming_requests_stat;
        stat::StatisticsCollector<int32_t> request_interval_stat;
        stat::StatisticsCollector<int32_t> end2end_delay_stat;
        explicit PerFuncStat(uint16_t func_id);
    };

    absl::Mutex mu_;

    absl::flat_hash_map</* full_call_id */ uint64_t, FuncCallState>
        running_func_calls_ ABSL_GUARDED_BY(mu_);
    std::deque<FuncCallState> pending_func_calls_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_set</* full_call_id */ uint64_t>
        discarded_func_calls_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* connection_id */ int,
                        std::shared_ptr<server::ConnectionBase>>
        connections_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::EgressHub>>
        engine_egress_hubs_ ABSL_GUARDED_BY(mu_);

    int64_t last_request_timestamp_ ABSL_GUARDED_BY(mu_);
    stat::Counter incoming_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<int32_t> request_interval_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<float> requests_instant_rps_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> inflight_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<uint16_t> running_requests_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<int32_t> queueing_delay_stat_ ABSL_GUARDED_BY(mu_);
    stat::StatisticsCollector<int32_t> dispatch_overhead_stat_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* func_id */ uint16_t, std::unique_ptr<PerFuncStat>>
        per_func_stats_ ABSL_GUARDED_BY(mu_);

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;
    void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake, int sockfd) override;

    void SetupHttpServer();
    void SetupGrpcServer();

    void OnNewFuncCallCommon(std::shared_ptr<server::ConnectionBase> parent_connection,
                             FuncCallContext* func_call_context);
    bool DispatchFuncCall(std::shared_ptr<server::ConnectionBase> parent_connection,
                          FuncCallContext* func_call_context, uint16_t node_id);
    bool DispatchAsyncFuncCall(const protocol::FuncCall& func_call, uint32_t logspace,
                               std::span<const char> input, uint16_t node_id);
    void FinishFuncCall(std::shared_ptr<server::ConnectionBase> parent_connection,
                        FuncCallContext* func_call_context);
    void TickNewFuncCall(uint16_t func_id, int64_t current_timestamp)
        ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    void TryDispatchingPendingFuncCalls();

    static std::string EncodeAsyncCallResult(const AsyncCallResult& result);
    void BackgroundThreadMain();

    bool SendMessageToEngine(uint16_t node_id, const protocol::GatewayMessage& message,
                             std::span<const char> payload);
    void HandleFuncCallCompleteOrFailedMessage(uint16_t node_id,
                                               const protocol::GatewayMessage& message,
                                               std::span<const char> payload);

    void OnNewHttpConnection(int sockfd);
    void OnNewGrpcConnection(int sockfd);

    server::EgressHub* CreateEngineEgressHub(uint16_t node_id, server::IOWorker* io_worker);

    DISALLOW_COPY_AND_ASSIGN(Server);
};

}  // namespace gateway
}  // namespace faas
