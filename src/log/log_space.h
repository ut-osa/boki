#pragma once

#include "log/log_space_base.h"

namespace faas {
namespace log {

// Used in Sequencer
class MetaLogPrimary final : public LogSpaceBase {
public:
    MetaLogPrimary(const View* view, uint16_t sequencer_id);
    ~MetaLogPrimary();

    void UpdateStorageProgress(uint16_t stroage_id,
                               const std::vector<uint32_t>& progress);
    void UpdateReplicaProgress(uint16_t sequencer_id, uint32_t metalog_position);
    bool MarkNextCut(MetaLogProto* meta_log_proto);

private:
    const View::Sequencer* sequencer_node_;

    DISALLOW_COPY_AND_ASSIGN(MetaLogPrimary);
};

// Used in Sequencer
class MetaLogBackup final : public LogSpaceBase {
public:
    MetaLogBackup(const View* view, uint16_t sequencer_id);
    ~MetaLogBackup();

private:
    DISALLOW_COPY_AND_ASSIGN(MetaLogBackup);
};

// Used in Engine
class LogProducer final : public LogSpaceBase {
public:
    LogProducer(uint16_t engine_id, const View* view, uint16_t sequencer_id);
    ~LogProducer();

    // Will store localid in Log_metadata
    void AppendLocally(LogMetaData* log_metadata,
                       std::span<const char> log_data);

    typedef std::function<void(uint64_t /* localid */, uint64_t /* user_seqnum */)>
            LogPersistedCallback;
    void SetLogPersistedCallback(LogPersistedCallback cb);

    typedef std::function<void(uint64_t /* localid */)>
            LogDiscardedCallback;
    void SetLogDiscardedCallback(LogDiscardedCallback cb);

private:
    void OnNewLogs(uint32_t start_seqnum, uint64_t start_localid,
                   uint32_t delta) override;
    void OnFinalized() override;

    DISALLOW_COPY_AND_ASSIGN(LogProducer);
};

// Used in Storage
class LogStorage final : public LogSpaceBase {
public:
    LogStorage(uint16_t storage_id, const View* view, uint16_t sequencer_id);
    ~LogStorage();

    bool Store(const LogMetaData& log_metadata, std::span<const char> log_data);
    void ReadAt(const protocol::SharedLogMessage& request);

    bool GrabLogEntriesForPersistence(
            std::vector<std::shared_ptr<const LogEntry>>* log_entries,
            uint32_t* new_position);
    void LogEntriesPersisted(uint32_t new_position);

    struct ReadResult {
        enum Status { kOK, kLookupDB, kFailed };
        Status status;
        std::shared_ptr<const LogEntry> log_entry;
        protocol::SharedLogMessage original_request;
    };
    typedef absl::InlinedVector<ReadResult, 4> ReadResultVec;
    void PollReadResults(ReadResultVec* results);

    bool GrabShardProgressForSending(std::vector<uint32_t>* progress);

private:
    const View::Storage* storage_node_;

    bool shard_progrss_dirty_;
    absl::flat_hash_map</* engine_id */ uint16_t,
                        /* localid */ uint32_t> shard_progrsses_;

    uint32_t persisted_seqnum_position_;
    std::deque<uint32_t> live_seqnums_;
    absl::flat_hash_map</* seqnum */ uint32_t,
                        std::shared_ptr<const LogEntry>>
        live_log_entries_;

    absl::flat_hash_map</* localid */ uint64_t,
                        std::unique_ptr<LogEntry>>
        pending_log_entries_;

    std::multimap</* seqnum */ uint32_t,
                  protocol::SharedLogMessage> pending_read_requests_;
    ReadResultVec pending_read_results_;

    void OnNewLogs(uint32_t start_seqnum, uint64_t start_localid,
                   uint32_t delta) override;
    void OnFinalized() override;

    void AdvanceShardProgress(uint16_t engine_id);
    void ShrinkLiveEntriesIfNeeded();

    DISALLOW_COPY_AND_ASSIGN(LogStorage);
};

}  // namespace log
}  // namespace faas
