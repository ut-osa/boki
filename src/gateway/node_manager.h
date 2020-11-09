#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/stat.h"

namespace faas {
namespace gateway {

class Server;
class EngineConnection;

class NodeManager {
public:
    explicit NodeManager(Server* server);
    ~NodeManager();

    size_t num_connected_node() const;

    void OnNewEngineConnection(EngineConnection* connection);
    void OnEngineConnectionClosed(EngineConnection* connection);

    bool PickNodeForNewFuncCall(const protocol::FuncCall& func_call, uint16_t* node_id);
    void FuncCallFinished(const protocol::FuncCall& func_call, uint16_t node_id);

    bool SendMessage(uint16_t node_id, const protocol::GatewayMessage& message,
                     std::span<const char> payload);

private:
    Server* server_;

    mutable absl::Mutex mu_;

    size_t max_running_requests_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_set</* full_call_id */ uint64_t> running_requests_ ABSL_GUARDED_BY(mu_);

    struct Node {
        uint16_t                       node_id;
        size_t                         inflight_requests;
        size_t                         active_connections;
        std::vector<EngineConnection*> connections;
        std::unique_ptr<stat::Counter> dispatched_requests_stat;
    };

    absl::flat_hash_map</* node_id */ uint16_t, std::unique_ptr<Node>>
        connected_nodes_ ABSL_GUARDED_BY(mu_);
    std::vector<Node*> connected_node_list_ ABSL_GUARDED_BY(mu_);

    absl::BitGen random_bit_gen_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* func_id */ uint16_t, size_t>
        next_dispatch_node_idx_  ABSL_GUARDED_BY(mu_);

    void NewConnectedNode(uint16_t node_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

    DISALLOW_COPY_AND_ASSIGN(NodeManager);
};

}  // namespace gateway
}  // namespace faas
