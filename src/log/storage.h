#pragma once

#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "log/log_space.h"
#include "log/utils.h"
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
    const uint16_t node_id_;

    server::NodeWatcher node_watcher_;
    ViewWatcher view_watcher_;

    std::string db_path_;
    std::unique_ptr<rocksdb::DB> db_;

    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        ingress_conns_;

    absl::Mutex conn_mu_;
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::EgressHub>>
        egress_hubs_ ABSL_GUARDED_BY(conn_mu_);

    absl::Mutex core_mu_;
    LogSpaceCollection<LogStorage> storage_collection_ ABSL_GUARDED_BY(core_mu_);

    absl::Mutex future_request_mu_ ABSL_ACQUIRED_AFTER(core_mu_);
    FutureRequests future_requests_ ABSL_GUARDED_BY(future_request_mu_);

    void SetupRocksDB();
    void SetupZKWatchers();

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;
    void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                             int sockfd) override;

    void OnViewCreated(const View* view);
    void OnViewFinalized(const FinalizedView* finalized_view);

    void HandleReadAtRequest(const protocol::SharedLogMessage& message);
    void HandleReplicateRequest(const protocol::SharedLogMessage& message,
                                std::span<const char> payload);
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload);
    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);

    void ProcessReadResults(const LogStorage::ReadResultVec& results);
    void ProcessReadFromDB(const protocol::SharedLogMessage& request);

    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload);
    bool SendEngineResponse(const protocol::SharedLogMessage& request,
                            protocol::SharedLogMessage* response,
                            std::span<const char> payload = std::span<const char>());

    void OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);
    bool SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                              const protocol::SharedLogMessage& message,
                              std::span<const char> payload);

    server::EgressHub* CreateEgressHub(protocol::ConnType conn_type,
                                       uint16_t dst_node_id,
                                       server::IOWorker* io_worker);

    static inline std::string GetDBKey(uint32_t logspace_id, uint32_t seqnum) {
        return fmt::format("{0:08x}{1:08x}", logspace_id, seqnum);
    }

    DISALLOW_COPY_AND_ASSIGN(Storage);
};

}  // namespace log
}  // namespace faas
