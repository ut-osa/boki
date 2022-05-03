#include "gateway/node_manager.h"

#include "gateway/flags.h"
#include "gateway/server.h"

#define log_header_ "NodeManager: "

namespace faas {
namespace gateway {

using server::NodeWatcher;

NodeManager::NodeManager(Server* server)
    : server_(server),
      max_running_requests_(0) {}

NodeManager::~NodeManager() {}

bool NodeManager::PickNodeForNewFuncCall(const protocol::FuncCall& func_call,
                                         std::set<uint16_t> node_constraint,
                                         uint16_t* node_id) {
    absl::MutexLock lk(&mu_);
    if (connected_node_list_.empty()) {
        return false;
    }
    if (max_running_requests_ > 0 && running_requests_.size() > max_running_requests_) {
        return false;
    }
    std::vector<Node*> preferred_nodes;
    for (uint16_t node_id : node_constraint) {
        if (connected_nodes_.contains(node_id)) {
            preferred_nodes.push_back(connected_nodes_.at(node_id).get());
        } else {
            HLOG_F(ERROR, "Cannot find engine node with ID {}", node_id);
        }
    }
    if (!node_constraint.empty() && preferred_nodes.empty()) {
        return false;
    }

    const std::vector<Node*>& target_nodes = preferred_nodes.empty() ? connected_node_list_
                                                                     : preferred_nodes;
    size_t idx;
    if (absl::GetFlag(FLAGS_lb_per_fn_round_robin)) {
        idx = (next_dispatch_node_idx_[func_call.func_id]++) % target_nodes.size();
    } else if (absl::GetFlag(FLAGS_lb_pick_least_load)) {
        auto iter = absl::c_min_element(
            target_nodes,
            [] (const Node* lhs, const Node* rhs) {
                return lhs->inflight_requests < rhs->inflight_requests;
            }
        );
        idx = static_cast<size_t>(iter - target_nodes.begin());
    } else {
        idx = absl::Uniform<size_t>(random_bit_gen_, 0, target_nodes.size());
    }
    Node* node = target_nodes[idx];
    node->inflight_requests++;
    node->dispatched_requests_stat.Tick();
    running_requests_.insert(func_call.full_call_id);
    *node_id = node->node_id;
    return true;
}

void NodeManager::FuncCallFinished(const protocol::FuncCall& func_call, uint16_t node_id) {
    absl::MutexLock lk(&mu_);
    if (!running_requests_.contains(func_call.full_call_id)) {
        return;
    }
    running_requests_.erase(func_call.full_call_id);
    if (!connected_nodes_.contains(node_id)) {
        return;
    }
    Node* node = connected_nodes_[node_id].get();
    node->inflight_requests--;
}

void NodeManager::OnNodeOnline(NodeWatcher::NodeType node_type, uint16_t node_id) {
    if (node_type != NodeWatcher::kEngineNode) {
        return;
    }
    std::unique_ptr<Node> node = std::make_unique<Node>(node_id);
    {
        absl::MutexLock lk(&mu_);
        DCHECK(!connected_nodes_.contains(node_id))
            << fmt::format("Engine node {} already exists", node_id);
        connected_node_list_.push_back(node.get());
        connected_nodes_[node_id] = std::move(node);
        max_running_requests_ = absl::GetFlag(FLAGS_max_running_requests)
                              * connected_nodes_.size();
        HLOG_F(INFO, "{} nodes connected", connected_nodes_.size());
    }
    server_->OnEngineNodeOnline(node_id);
}

void NodeManager::OnNodeOffline(NodeWatcher::NodeType node_type, uint16_t node_id) {
    if (node_type != NodeWatcher::kEngineNode) {
        return;
    }
    {
        absl::MutexLock lk(&mu_);
        DCHECK(connected_nodes_.contains(node_id));
        connected_nodes_.erase(node_id);
        connected_node_list_.clear();
        for (auto& entry : connected_nodes_) {
            connected_node_list_.push_back(entry.second.get());
        }
        max_running_requests_ = absl::GetFlag(FLAGS_max_running_requests)
                              * connected_nodes_.size();
        HLOG_F(INFO, "{} nodes connected", connected_nodes_.size());
    }
    server_->OnEngineNodeOffline(node_id);
}

NodeManager::Node::Node(uint16_t node_id)
    : node_id(node_id),
      inflight_requests(0),
      dispatched_requests_stat(stat::Counter::StandardReportCallback(
          fmt::format("dispatched_requests[{}]", node_id))) {}

}  // namespace gateway
}  // namespace faas
