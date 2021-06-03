#pragma once

#include "log/storage_base.h"
#include "log/log_space.h"
#include "log/utils.h"
#include "common/stat.h"

namespace faas {
namespace log {

class Storage final : public StorageBase {
public:
    explicit Storage(uint16_t node_id);
    ~Storage();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_      ABSL_GUARDED_BY(view_mu_);
    bool view_finalized_           ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogStorage>
        storage_collection_        ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;

    absl::Mutex flush_mu_;
    absl::InlinedVector<const LogStorage::Entry*, 32>
        log_entires_for_flush_     ABSL_GUARDED_BY(flush_mu_);
    stat::StatisticsCollector<int>
        log_entires_flush_stat_    ABSL_GUARDED_BY(flush_mu_);

    stat::CategoryCounter flush_thread_entry_src_stat_;

    void OnViewCreated(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleReadAtRequest(const protocol::SharedLogMessage& request) override;
    void HandleReplicateRequest(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;
    void OnRecvLogAuxData(const protocol::SharedLogMessage& message,
                          std::span<const char> payload) override;

    void ProcessReadResults(const LogStorage::ReadResultVec& results);
    void ProcessReadFromDB(const protocol::SharedLogMessage& request);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void SendEngineLogResult(const protocol::SharedLogMessage& request,
                             protocol::SharedLogMessage* response,
                             std::span<const char> tags_data,
                             std::span<const char> log_data);
    void SendEngineLogResult(const protocol::SharedLogMessage& request,
                             protocol::SharedLogMessage* response,
                             const LogEntry& log_entry);

    void BackgroundThreadMain(int eventfd) override;
    void SendShardProgressIfNeeded() override;
    void FlushLogEntries();

    LogEntry ReadLogEntryFromJournal(uint64_t seqnum,
                                     server::JournalFile* file, size_t offset);

    DISALLOW_COPY_AND_ASSIGN(Storage);
};

}  // namespace log
}  // namespace faas
