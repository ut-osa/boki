#include "server/node_watcher.h"

namespace faas {
namespace server {

NodeWatcher::NodeWatcher() {}

NodeWatcher::~NodeWatcher() {}

void NodeWatcher::StartWatching(zk::ZKSession* session) {
    watcher_.reset(new zk_utils::DirWatcher(session, "node_addr"));
    watcher_->SetNodeCreatedCallback(
        absl::bind_front(&NodeWatcher::OnZNodeCreated, this));
    watcher_->SetNodeChangedCallback(
        absl::bind_front(&NodeWatcher::OnZNodeChanged, this));
    watcher_->SetNodeDeletedCallback(
        absl::bind_front(&NodeWatcher::OnZNodeDeleted, this));
    watcher_->Start();
}

void NodeWatcher::SetNodeOnlineCallback(NodeEventCallback cb) {
    node_online_cb_ = cb;
}

void NodeWatcher::SetNodeOfflineCallback(NodeEventCallback cb) {
    node_offline_cb_ = cb;
}

bool NodeWatcher::GetNodeAddr(NodeType node_type, uint16_t node_id,
                              struct sockaddr_in* addr) {
    absl::MutexLock lk(&mu_);
    if (!node_addr_[node_type].contains(node_id)) {
        return false;
    }
    *addr = node_addr_[node_type][node_id];
    return true;
}

bool NodeWatcher::ParseNodePath(std::string_view path,
                                NodeType* node_type, uint16_t* node_id) {
    if (path == "gateway") {
        *node_type = kGatewayNode;
        *node_id = 0;
        return true;
    }
    std::string_view prefix;
    if (absl::StartsWith(path, "engine_")) {
        prefix = "engine_";
        *node_type = kEngineNode;
    } else if (absl::StartsWith(path, "sequencer_")) {
        prefix = "sequencer_";
        *node_type = kSequencerNode;
    } else {
        LOG(ERROR) << "Unknown type of node: " << path;
        return false;
    }
    int parsed;
    if (!absl::SimpleAtoi(absl::StripPrefix(path, prefix), &parsed)) {
        LOG(ERROR) << "Failed to parse node_id: " << path;
        return false;
    }
    *node_id = gsl::narrow_cast<uint16_t>(parsed);
    return true;
}

void NodeWatcher::OnZNodeCreated(std::string_view path, std::span<const char> contents) {
    NodeType node_type;
    uint16_t node_id;
    CHECK(ParseNodePath(path, &node_type, &node_id));
    std::string_view addr_str(contents.data(), contents.size());
    struct sockaddr_in addr;
    if (!utils::ResolveTcpAddr(&addr, addr_str)) {
        LOG(FATAL) << fmt::format("Cannot resolve address for node {}: {}",
                                   node_id, addr_str);
    }
    {
        absl::MutexLock lk(&mu_);
        CHECK(!node_addr_[node_type].contains(node_id))
            << fmt::format("{} {} already exists", kNodeTypeStr[node_type], node_id);
        node_addr_[node_type][node_id] = addr;
    }
    if (node_online_cb_) {
        node_online_cb_(node_type, node_id);
    }
}

void NodeWatcher::OnZNodeChanged(std::string_view path, std::span<const char> contents) {
    LOG(FATAL) << fmt::format("Contents of znode {} changed", path);
}

void NodeWatcher::OnZNodeDeleted(std::string_view path) {
    NodeType node_type;
    uint16_t node_id;
    CHECK(ParseNodePath(path, &node_type, &node_id));
    {
        absl::MutexLock lk(&mu_);
        CHECK(node_addr_[node_type].contains(node_id))
            << fmt::format("{} {} does not exist", kNodeTypeStr[node_type], node_id);
        node_addr_[node_type].erase(node_id);
    }
    if (node_offline_cb_) {
        node_offline_cb_(node_type, node_id);
    }
}

}  // namespace server
}  // namespace faas
