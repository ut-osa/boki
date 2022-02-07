#pragma once

#include "log/storage_base.h"
#include "log/log_space.h"
#include "log/utils.h"

namespace faas {
namespace log {

class Storage final : public StorageBase {
public:
    explicit Storage(uint16_t node_id);
    ~Storage() = default;

private:
    std::string log_header_;
    bool enable_db_staging_;

    absl::Mutex                    view_mu_;
    const View*                    current_view_        ABSL_GUARDED_BY(view_mu_);
    bool                           view_finalized_      ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogStorage> storage_collection_  ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;

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
    void ProcessReadFromJournal(const protocol::SharedLogMessage& request);
    void ProcessReadFromDB(const protocol::SharedLogMessage& request);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void SendEngineLogResult(const protocol::SharedLogMessage& request,
                             protocol::SharedLogMessage* response,
                             const LogMetaData& metadata,
                             std::span<const char> tags_data,
                             std::span<const char> log_data);
    void SendEngineLogResult(const protocol::SharedLogMessage& request,
                             protocol::SharedLogMessage* response,
                             const LogEntry& log_entry);

    LogEntry ReadInlinedLogEntry(uint64_t seqnum, uint64_t localid,
                                 const LogStorage::LogData& data);

    void SendShardProgressIfNeeded() override;
    void CollectLogTrimOps() override;
    void FlushLogEntries(std::span<const LogStorage::Entry* const> entries) override;
    void CommitLogEntries(std::span<const LogStorage::Entry* const> entries) override;

    DISALLOW_COPY_AND_ASSIGN(Storage);
};

}  // namespace log
}  // namespace faas
