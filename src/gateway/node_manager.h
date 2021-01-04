#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "common/zk.h"
#include "common/zk_utils.h"
#include "utils/socket.h"

namespace faas {
namespace gateway {

class Server;
class EngineConnection;

class NodeManager {
public:
    explicit NodeManager(Server* server);
    ~NodeManager();

    void StartWatchingEngineNodes(zk::ZKSession* session);

    bool PickNodeForNewFuncCall(const protocol::FuncCall& func_call, uint16_t* node_id);
    void FuncCallFinished(const protocol::FuncCall& func_call, uint16_t node_id);

    bool GetNodeAddr(uint16_t node_id, struct sockaddr_in* addr);

private:
    Server* server_;
    std::unique_ptr<zk_utils::DirWatcher> engine_watcher_;

    absl::Mutex mu_;

    size_t max_running_requests_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_set</* full_call_id */ uint64_t> running_requests_ ABSL_GUARDED_BY(mu_);

    struct Node {
        uint16_t node_id;
        struct sockaddr_in addr;
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

    void OnZNodeCreated(std::string_view path, std::span<const char> contents);
    void OnZNodeChanged(std::string_view path, std::span<const char> contents);
    void OnZNodeDeleted(std::string_view path);

    DISALLOW_COPY_AND_ASSIGN(NodeManager);
};

}  // namespace gateway
}  // namespace faas
