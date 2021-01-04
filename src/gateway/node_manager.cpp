#include "gateway/node_manager.h"

#include "gateway/flags.h"
#include "gateway/server.h"

#define log_header_ "NodeManager: "

namespace faas {
namespace gateway {

NodeManager::NodeManager(Server* server)
    : server_(server),
      max_running_requests_(0) {}

NodeManager::~NodeManager() {}

void NodeManager::StartWatchingEngineNodes(zk::ZKSession* session) {
    engine_watcher_.reset(new zk_utils::DirWatcher(session, "engines"));
    engine_watcher_->SetNodeCreatedCallback(
        absl::bind_front(&NodeManager::OnZNodeCreated, this));
    engine_watcher_->SetNodeChangedCallback(
        absl::bind_front(&NodeManager::OnZNodeChanged, this));
    engine_watcher_->SetNodeDeletedCallback(
        absl::bind_front(&NodeManager::OnZNodeDeleted, this));
    engine_watcher_->Start();
}

bool NodeManager::PickNodeForNewFuncCall(const protocol::FuncCall& func_call, uint16_t* node_id) {
    absl::MutexLock lk(&mu_);
    if (connected_node_list_.empty()) {
        return false;
    }
    if (max_running_requests_ > 0 && running_requests_.size() > max_running_requests_) {
        return false;
    }
    size_t idx;
    if (absl::GetFlag(FLAGS_lb_per_fn_round_robin)) {
        idx = (next_dispatch_node_idx_[func_call.func_id]++) % connected_node_list_.size();
    } else if (absl::GetFlag(FLAGS_lb_pick_least_load)) {
        idx = absl::c_min_element(connected_node_list_, [] (const Node* lhs, const Node* rhs) {
            return lhs->inflight_requests < rhs->inflight_requests;
        }) - connected_node_list_.begin();
    } else {
        idx = absl::Uniform<size_t>(random_bit_gen_, 0, connected_node_list_.size());
    }
    Node* node = connected_node_list_[idx];
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

bool NodeManager::GetNodeAddr(uint16_t node_id, struct sockaddr_in* addr) {
    absl::MutexLock lk(&mu_);
    if (!connected_nodes_.contains(node_id)) {
        return false;
    }
    Node* node = connected_nodes_[node_id].get();
    memcpy(addr, &node->addr, sizeof(struct sockaddr_in));
    return true;
}

NodeManager::Node::Node(uint16_t node_id)
    : node_id(node_id),
      inflight_requests(0),
      dispatched_requests_stat(stat::Counter::StandardReportCallback(
          fmt::format("dispatched_requests[{}]", node_id))) {}

void NodeManager::OnZNodeCreated(std::string_view path, std::span<const char> contents) {
    int parsed;
    CHECK(absl::SimpleAtoi(path, &parsed));
    uint16_t node_id = gsl::narrow_cast<uint16_t>(parsed);
    std::string_view engine_host;
    uint16_t port;
    CHECK(utils::ParseHostPort(
        std::string_view(contents.data(), contents.size()),
        &engine_host, &port));
    std::unique_ptr<Node> node = std::make_unique<Node>(node_id);
    CHECK(utils::FillTcpSocketAddr(&node->addr, engine_host, port));
    {
        absl::MutexLock lk(&mu_);
        DCHECK(!connected_nodes_.contains(node_id))
            << fmt::format("Engine node {} already exists", node_id);
        connected_node_list_.push_back(node.get());
        connected_nodes_[node_id] = std::move(node);
    }
    server_->OnEngineNodeOnline(node_id);
}

void NodeManager::OnZNodeChanged(std::string_view path, std::span<const char> contents) {
    LOG(FATAL) << fmt::format("Contents of znode {} changed", path);
}

void NodeManager::OnZNodeDeleted(std::string_view path) {
    int parsed;
    CHECK(absl::SimpleAtoi(path, &parsed));
    uint16_t node_id = gsl::narrow_cast<uint16_t>(parsed);
    {
        absl::MutexLock lk(&mu_);
        DCHECK(connected_nodes_.contains(node_id));
        connected_nodes_.erase(node_id);
        connected_node_list_.clear();
        for (auto& entry : connected_nodes_) {
            connected_node_list_.push_back(entry.second.get());
        }
    }
    server_->OnEngineNodeOffline(node_id);
}

}  // namespace gateway
}  // namespace faas
