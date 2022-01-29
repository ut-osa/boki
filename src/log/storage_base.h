#pragma once

#include "base/thread.h"
#include "common/stat.h"
#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "log/db.h"
#include "log/cache.h"
#include "log/log_space.h"
#include "log/storage_indexer.h"
#include "log/db_workers.h"
#include "server/server_base.h"
#include "server/ingress_connection.h"
#include "server/egress_hub.h"
#include "server/journal.h"

namespace faas {
namespace log {

class StorageBase : public server::ServerBase {
public:
    explicit StorageBase(uint16_t node_id);
    virtual ~StorageBase();

    void set_db_path(std::string_view path) { db_path_ = std::string(path); }

    bool running() const {
        return state_.load(std::memory_order_acquire) != kStopping;
    }

protected:
    uint16_t my_node_id() const { return node_id_; }

    virtual void OnViewCreated(const View* view) = 0;
    virtual void OnViewFinalized(const FinalizedView* finalized_view) = 0;

    virtual void HandleReadAtRequest(const protocol::SharedLogMessage& request) = 0;
    virtual void HandleReplicateRequest(const protocol::SharedLogMessage& message,
                                        std::span<const char> payload) = 0;
    virtual void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                                   std::span<const char> payload) = 0;
    virtual void OnRecvLogAuxData(const protocol::SharedLogMessage& message,
                                  std::span<const char> payload) = 0;

    virtual void FlushLogEntries(std::span<const LogStorage::Entry*> entries) = 0;
    virtual void CommitLogEntries(std::span<const LogStorage::Entry*> entries) = 0;
    virtual void SendShardProgressIfNeeded() = 0;
    virtual void CollectLogTrimOps() = 0;

    bool db_enabled() { return !journal_enabled(); }

    inline DBInterface* log_db() { return DCHECK_NOTNULL(db_.get()); }

    inline LRUCache* log_cache() {
        DCHECK(log_cache_.has_value());
        return &log_cache_.value();
    }

    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);
    std::optional<LogEntryProto> GetLogEntryFromDB(uint64_t seqnum);

    void SendIndexData(const View* view, const IndexDataProto& index_data_proto);
    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload);
    bool SendEngineResponse(const protocol::SharedLogMessage& request,
                            protocol::SharedLogMessage* response,
                            std::span<const char> payload1 = EMPTY_CHAR_SPAN,
                            std::span<const char> payload2 = EMPTY_CHAR_SPAN,
                            std::span<const char> payload3 = EMPTY_CHAR_SPAN);

    inline DBWorkers* db_workers() {
        DCHECK(db_workers_.has_value());
        return &db_workers_.value();
    }

    inline StorageIndexer* indexer() {
        DCHECK(indexer_.has_value());
        return &indexer_.value();
    }

    std::optional<server::JournalFileRef> GetJournalFile(int file_id);

private:
    friend class DBWorkers;

    const uint16_t node_id_;

    ViewWatcher view_watcher_;

    std::string db_path_;
    std::unique_ptr<DBInterface> db_;

    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        ingress_conns_;

    absl::Mutex conn_mu_;
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::EgressHub>>
        egress_hubs_ ABSL_GUARDED_BY(conn_mu_);

    absl::Mutex journal_file_mu_;
    absl::flat_hash_map</* file_id */ int, server::JournalFileRef>
        journal_files_ ABSL_GUARDED_BY(journal_file_mu_);

    std::optional<LRUCache>       log_cache_;
    std::optional<DBWorkers>      db_workers_;
    std::optional<StorageIndexer> indexer_;

    void SetupDB();
    void SetupZKWatchers();
    void SetupTimers();

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;
    void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                             int sockfd) override;

    void OnNewJournalFile(server::JournalFile* file) override;

    void OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);
    bool SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                              const protocol::SharedLogMessage& message,
                              std::span<const char> payload1,
                              std::span<const char> payload2 = EMPTY_CHAR_SPAN,
                              std::span<const char> payload3 = EMPTY_CHAR_SPAN);

    server::EgressHub* CreateEgressHub(protocol::ConnType conn_type,
                                       uint16_t dst_node_id,
                                       server::IOWorker* io_worker);

    DISALLOW_COPY_AND_ASSIGN(StorageBase);
};

}  // namespace log
}  // namespace faas
