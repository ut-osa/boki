#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/zk.h"
#include "common/zk_utils.h"
#include "utils/socket.h"

namespace faas {
namespace server {

class NodeWatcher final {
public:
    NodeWatcher();
    ~NodeWatcher() = default;

    void StartWatching(zk::ZKSession* session);

    enum NodeType {
        kGatewayNode   = 0,
        kEngineNode    = 1,
        kSequencerNode = 2,
        kStorageNode   = 3,
        kTotalNodeType = 4
    };

    using NodeEventCallback = std::function<void(NodeType /* node_type */, uint16_t node_id)>;
    void SetNodeOnlineCallback(NodeEventCallback cb);
    void SetNodeOfflineCallback(NodeEventCallback cb);

    bool GetNodeAddr(NodeType node_type, uint16_t node_id, struct sockaddr_in* addr);

    static NodeType GetSrcNodeType(protocol::ConnType conn_type);
    static NodeType GetDstNodeType(protocol::ConnType conn_type);

private:
    static constexpr const char* kNodeTypeStr[] = {
        "GatewayNode",
        "EngineNode",
        "SequencerNode",
        "StorageNode"
    };

    std::optional<zk_utils::DirWatcher> watcher_;

    NodeEventCallback node_online_cb_;
    NodeEventCallback node_offline_cb_;

    absl::Mutex mu_;
    absl::flat_hash_map</* node_id */ uint16_t, struct sockaddr_in>
        node_addr_[kTotalNodeType] ABSL_GUARDED_BY(mu_);

    bool ParseNodePath(std::string_view path, NodeType* node_type, uint16_t* node_id);

    void OnZNodeCreated(std::string_view path, std::span<const char> contents);
    void OnZNodeChanged(std::string_view path, std::span<const char> contents);
    void OnZNodeDeleted(std::string_view path);

    DISALLOW_COPY_AND_ASSIGN(NodeWatcher);
};

}  // namespace server
}  // namespace faas
