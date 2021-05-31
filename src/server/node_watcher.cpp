#include "server/node_watcher.h"

namespace faas {
namespace server {

NodeWatcher::NodeWatcher() {}

NodeWatcher::~NodeWatcher() {}

void NodeWatcher::StartWatching(zk::ZKSession* session) {
    watcher_.emplace(session, "node");
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
    } else if (absl::StartsWith(path, "storage_")) {
        prefix = "storage_";
        *node_type = kStorageNode;
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
    LOG_F(INFO, "Seen new node {} with address {}", path, addr_str);
    struct sockaddr_in addr;
    if (!utils::ResolveTcpAddr(&addr, addr_str)) {
        LOG_F(FATAL, "Cannot resolve address for node {}: {}", node_id, addr_str);
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
    LOG_F(FATAL, "Contents of znode {} changed", path);
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

namespace {
using protocol::ConnType;
typedef std::pair<NodeWatcher::NodeType, NodeWatcher::NodeType> NodeTypePair;
#define NODE_PAIR(A, B) { NodeWatcher::k##A##Node, NodeWatcher::k##B##Node }

const std::unordered_map<ConnType, NodeTypePair> kNodeTypeTable {
    { ConnType::GATEWAY_TO_ENGINE,      NODE_PAIR(Gateway, Engine) },
    { ConnType::ENGINE_TO_GATEWAY,      NODE_PAIR(Engine, Gateway) },
    { ConnType::SLOG_ENGINE_TO_ENGINE,  NODE_PAIR(Engine, Engine) },
    { ConnType::ENGINE_TO_SEQUENCER,    NODE_PAIR(Engine, Sequencer) },
    { ConnType::SEQUENCER_TO_ENGINE,    NODE_PAIR(Sequencer, Engine) },
    { ConnType::SEQUENCER_TO_SEQUENCER, NODE_PAIR(Sequencer, Sequencer) },
    { ConnType::ENGINE_TO_STORAGE,      NODE_PAIR(Engine, Storage) },
    { ConnType::STORAGE_TO_ENGINE,      NODE_PAIR(Storage, Engine) },
    { ConnType::SEQUENCER_TO_STORAGE,   NODE_PAIR(Sequencer, Storage) },
    { ConnType::STORAGE_TO_SEQUENCER,   NODE_PAIR(Storage, Sequencer) },
};

#undef NODE_PAIR

}  // namespace

NodeWatcher::NodeType NodeWatcher::GetSrcNodeType(protocol::ConnType conn_type) {
    CHECK(kNodeTypeTable.count(conn_type) > 0);
    return kNodeTypeTable.at(conn_type).first;
}

NodeWatcher::NodeType NodeWatcher::GetDstNodeType(protocol::ConnType conn_type) {
    CHECK(kNodeTypeTable.count(conn_type) > 0);
    return kNodeTypeTable.at(conn_type).second;
}

}  // namespace server
}  // namespace faas
