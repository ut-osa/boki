#pragma once

#include "base/thread.h"
#include "common/stat.h"
#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "log/db.h"
#include "log/cache.h"
#include "log/log_space.h"
#include "server/server_base.h"
#include "server/ingress_connection.h"
#include "server/egress_hub.h"

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

    inline LRUCache* log_cache() {
        DCHECK(log_cache_.has_value());
        return &log_cache_.value();
    }

    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);
    std::optional<LogEntryProto> GetLogEntryFromDB(uint64_t seqnum);
    void PutLogEntryToDB(const LogEntry& log_entry);

    void SendIndexData(const View* view, const IndexDataProto& index_data_proto);
    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload);
    bool SendEngineResponse(const protocol::SharedLogMessage& request,
                            protocol::SharedLogMessage* response,
                            std::span<const char> payload1 = EMPTY_CHAR_SPAN,
                            std::span<const char> payload2 = EMPTY_CHAR_SPAN,
                            std::span<const char> payload3 = EMPTY_CHAR_SPAN);

    class DBFlusher {
    public:
        explicit DBFlusher(StorageBase* storage, size_t num_worker_threads);
        ~DBFlusher();

        void PushLogEntriesForFlush(std::span<const LogStorage::Entry*> entries);

        void SignalAllThreads();
        void JoinAllThreads();

    private:
        static constexpr size_t kBatchSize = 128;

        StorageBase* storage_;
        absl::FixedArray<std::optional<base::Thread>> worker_threads_;

        absl::Mutex mu_;
        absl::CondVar cv_;

        int idle_workers_ ABSL_GUARDED_BY(mu_);
        std::deque<std::pair</* seqnum */ uint64_t,
                             const LogStorage::Entry*>>
            queue_ ABSL_GUARDED_BY(mu_);
        uint64_t next_seqnum_ ABSL_GUARDED_BY(mu_);

        absl::Mutex commit_mu_;
        uint64_t commited_seqnum_    ABSL_GUARDED_BY(commit_mu_);
        std::map</* seqnum */ uint64_t, const LogStorage::Entry*>
            flushed_entries_         ABSL_GUARDED_BY(commit_mu_);

        stat::StatisticsCollector<int> queue_length_stat_ ABSL_GUARDED_BY(mu_);

        bool should_stop() { return !storage_->running(); }
        void WorkerThreadMain(int thread_index);

        DISALLOW_COPY_AND_ASSIGN(DBFlusher);
    };

    inline DBFlusher* db_flusher() {
        DCHECK(db_flusher_.has_value());
        return &db_flusher_.value();
    }

private:
    const uint16_t node_id_;

    ViewWatcher view_watcher_;

    std::string db_path_;
    std::unique_ptr<DBInterface> db_;

    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        ingress_conns_;

    absl::Mutex conn_mu_;
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::EgressHub>>
        egress_hubs_ ABSL_GUARDED_BY(conn_mu_);

    std::optional<LRUCache> log_cache_;
    std::optional<DBFlusher> db_flusher_;

    void SetupDB();
    void SetupZKWatchers();
    void SetupTimers();

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;
    void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                             int sockfd) override;

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
