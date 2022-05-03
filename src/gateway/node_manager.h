#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "server/node_watcher.h"

namespace faas {
namespace gateway {

class Server;

class NodeManager {
public:
    explicit NodeManager(Server* server);
    ~NodeManager();

    bool PickNodeForNewFuncCall(const protocol::FuncCall& func_call,
                                std::set<uint16_t> node_constraint,
                                uint16_t* node_id);
    void FuncCallFinished(const protocol::FuncCall& func_call, uint16_t node_id);

    void OnNodeOnline(server::NodeWatcher::NodeType node_type, uint16_t node_id);
    void OnNodeOffline(server::NodeWatcher::NodeType node_type, uint16_t node_id);

private:
    Server* server_;

    absl::Mutex mu_;

    size_t max_running_requests_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_set</* full_call_id */ uint64_t> running_requests_ ABSL_GUARDED_BY(mu_);

    struct Node {
        uint16_t node_id;
        size_t inflight_requests;
        stat::Counter dispatched_requests_stat;
        explicit Node(uint16_t node_id);
    };

    absl::flat_hash_map</* node_id */ uint16_t, std::unique_ptr<Node>>
        connected_nodes_ ABSL_GUARDED_BY(mu_);
    std::vector<Node*> connected_node_list_ ABSL_GUARDED_BY(mu_);

    absl::BitGen random_bit_gen_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* func_id */ uint16_t, size_t>
        next_dispatch_node_idx_  ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(NodeManager);
};

}  // namespace gateway
}  // namespace faas
