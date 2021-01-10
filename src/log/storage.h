#pragma once

#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "server/server_base.h"
#include "server/ingress_connection.h"
#include "server/egress_hub.h"

#include <rocksdb/db.h>

namespace faas {
namespace log {

class Storage final : public server::ServerBase {
public:
    explicit Storage(uint16_t node_id);
    ~Storage();

    void set_db_path(std::string_view path) { db_path_ = std::string(path); }

private:
    uint16_t node_id_;

    server::NodeWatcher node_watcher_;
    ViewWatcher view_watcher_;

    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;

    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        ingress_conns_;

    absl::Mutex mu_;

    absl::flat_hash_map</* id */ int, std::unique_ptr<server::EgressHub>>
        egress_hubs_ ABSL_GUARDED_BY(mu_);

    void SetupRocksDB();
    void SetupZKWatchers();

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;
    void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                             int sockfd) override;

    void OnViewCreated(const View* view);
    void OnViewFinalized(const FinalizedView* finalized_view);

    void OnRecvSequencerMessage(uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);
    void OnRecvEngineMessage(uint16_t src_node_id,
                             const protocol::SharedLogMessage& message,
                             std::span<const char> payload);

    bool SendSequencerMessage(uint16_t dst_node_id,
                              const protocol::SharedLogMessage& message,
                              std::span<const char> payload);
    bool SendEngineMessage(uint16_t dst_node_id,
                           const protocol::SharedLogMessage& message,
                           std::span<const char> payload);

    bool SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                              const protocol::SharedLogMessage& message,
                              std::span<const char> payload);
    void OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);
    server::EgressHub* CreateEgressHub(protocol::ConnType conn_type,
                                       uint16_t dst_node_id,
                                       server::IOWorker* io_worker);

    DISALLOW_COPY_AND_ASSIGN(Storage);
};

}  // namespace log
}  // namespace faas
