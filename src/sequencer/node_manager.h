#pragma once

#include "base/common.h"
#include "common/uv.h"
#include "common/stat.h"
#include "common/protocol.h"
#include "utils/buffer_pool.h"
#include "utils/object_pool.h"

namespace faas {
namespace sequencer {

class Server;

class NodeManager : public uv::Base {
public:
    static constexpr size_t kBufferSize = 65536;
    static constexpr int kListenBackLog = 64;

    explicit NodeManager(Server* server);
    ~NodeManager();

    void Start(uv_loop_t* uv_loop, std::string_view listen_addr, uint16_t listen_port);
    void ScheduleStop();

    bool SendMessage(uint16_t node_id, const protocol::SequencerMessage& message,
                     std::span<const char> payload);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };

    Server* server_;
    State state_;
    std::string log_header_;

    uv_loop_t* uv_loop_;
    uv_tcp_t uv_handle_;

    utils::BufferPool buffer_pool_;
    utils::SimpleObjectPool<uv_write_t> write_req_pool_;

    class Connection;
    friend class Connection;
    absl::flat_hash_set<std::unique_ptr<Connection>> connections_;    

    struct NodeContext {
        uint16_t node_id;
        absl::flat_hash_set<Connection*> active_connections;
        absl::flat_hash_set<Connection*>::iterator next_connection;
    };
    absl::flat_hash_map</* node_id */ uint16_t, std::unique_ptr<NodeContext>>
        connected_nodes_;

    void OnConnectionHandshaked(Connection* connection);
    void OnConnectionClosing(Connection* connection);
    void OnConnectionClosed(Connection* connection);

    DECLARE_UV_CONNECTION_CB_FOR_CLASS(EngineConnection);

    DISALLOW_COPY_AND_ASSIGN(NodeManager);
};

}  // namespace sequencer
}  // namespace faas
