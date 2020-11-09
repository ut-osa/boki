#pragma once

#include "base/common.h"

namespace faas {
namespace gateway {

class Server;

class SharedLog {
public:
    explicit SharedLog(Server* server);
    ~SharedLog();

    void OnNewNodeConnected(uint16_t node_id, std::string_view shared_log_addr);
    void OnNodeMessage(uint16_t node_id, std::span<const char> data);

private:
    Server* server_;
    size_t num_replicas_;

    mutable absl::Mutex mu_;

    struct Node {
        uint16_t     node_id;
        int32_t      next_msg_seqnum;
        std::string  shared_log_addr;
    };
    absl::flat_hash_map</* node_id */ uint16_t, std::unique_ptr<Node>>
        nodes_ ABSL_GUARDED_BY(mu_);
    
    struct GlobalCut {
        uint64_t              start_seqnum;
        std::vector<uint32_t> cuts;
    };
    struct View {
        uint16_t               view_id;
        std::vector<uint16_t>  nodes;
        std::vector<GlobalCut> global_cuts;
    };
    std::vector<std::unique_ptr<View>> views_;

    View* current_view_;

    DISALLOW_COPY_AND_ASSIGN(SharedLog);
};

}  // namespace gateway
}  // namespace faas
