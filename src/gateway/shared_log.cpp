#include "gateway/shared_log.h"

#include "common/flags.h"

#define HLOG(l) LOG(l) << "SharedLog: "
#define HVLOG(l) VLOG(l) << "SharedLog: "

namespace faas {
namespace gateway {

SharedLog::SharedLog(Server* server)
    : server_(server),
      num_replicas_(absl::GetFlag(FLAGS_shared_log_num_replicas)),
      current_view_(nullptr) {
}

SharedLog::~SharedLog() {
}

void SharedLog::OnNewNodeConnected(uint16_t node_id, std::string_view shared_log_addr) {
    std::unique_ptr<Node> node(new Node);
    node->node_id = node_id;
    node->next_msg_seqnum = 0;
    node->shared_log_addr = std::string(shared_log_addr);
    absl::MutexLock lk(&mu_);
    DCHECK(!nodes_.contains(node_id));
    nodes_[node_id] = std::move(node);
    if (nodes_.size() >= num_replicas_) {
        
    }
}

void SharedLog::OnNodeMessage(uint16_t node_id, std::span<const char> data) {

}

}  // namespace gateway
}  // namespace faas
