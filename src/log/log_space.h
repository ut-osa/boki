#pragma once

#include "log/log_space_base.h"
#include "common/stat.h"

namespace faas {
namespace log {

// Used in Sequencer
class MetaLogPrimary final : public LogSpaceBase {
public:
    MetaLogPrimary(const View* view, uint16_t sequencer_id);
    ~MetaLogPrimary();

    void UpdateStorageProgress(uint16_t storage_id,
                               const std::vector<uint32_t>& progress);
    void UpdateReplicaProgress(uint16_t sequencer_id, uint32_t metalog_position);
    void MarkMetaLogPersisted(uint32_t metalog_seqnum);

    void GrabNewlyReplicatedMetalogs(MetaLogProtoVec* newly_replicated_metalogs);

    std::optional<MetaLogProto> MarkNextCut();
    MetaLogProto AppendTrimOp(const MetaLogProto::TrimProto& trim_op);

private:
    absl::flat_hash_set</* engine_id */ uint16_t> dirty_shards_;
    absl::flat_hash_map</* engine_id */ uint16_t, uint32_t> last_cut_;
    absl::flat_hash_map<std::pair</* engine_id */  uint16_t,
                                  /* storage_id */ uint16_t>,
                        uint32_t> shard_progrsses_;

    std::set<uint32_t> persisted_metalog_seqnums_;
    uint32_t persisted_metalog_position_;

    absl::flat_hash_map</* sequencer_id */ uint16_t,
                        uint32_t> metalog_progresses_;
    uint32_t replicated_metalog_position_;
    MetaLogProtoVec replicated_metalogs_;

    std::optional<uint32_t> previous_cut_seqnum_;

    uint32_t GetShardReplicatedPosition(uint16_t engine_id) const;
    void UpdateMetaLogReplicatedPosition();

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

    void LocalAppend(void* caller_data, uint64_t* localid);

    struct AppendResult {
        uint64_t seqnum;   // seqnum == kInvalidLogSeqNum indicates failure
        uint64_t localid;
        uint64_t metalog_progress;
        void*    caller_data;
    };
    using AppendResultVec = absl::InlinedVector<AppendResult, 4>;
    void PollAppendResults(AppendResultVec* results);

private:
    uint64_t next_localid_;
    absl::flat_hash_map</* localid */ uint64_t,
                        /* caller_data */ void*> pending_appends_;
    AppendResultVec pending_append_results_;

    void OnNewLogs(uint32_t metalog_seqnum,
                   uint64_t start_seqnum, uint64_t start_localid,
                   uint32_t delta) override;
    void OnFinalized(uint32_t metalog_position) override;

    DISALLOW_COPY_AND_ASSIGN(LogProducer);
};

// Used in Storage
class LogStorage final : public LogSpaceBase {
public:
    LogStorage(uint16_t storage_id, const View* view, uint16_t sequencer_id);
    ~LogStorage();

    using LogData = std::variant<std::string, JournalRecord>;

    struct Entry {
        int64_t      recv_timestamp;
        LogMetaData  metadata;
        UserTagVec   user_tags;
        LogData      data;
    };
    bool Store(Entry new_entry);
    void ReadAt(const protocol::SharedLogMessage& request);

    using LogEntryVec = absl::InlinedVector<const Entry*, 4>;
    void GrabLogEntriesForPersistence(LogEntryVec* log_entires);
    void LogEntriesPersisted(uint64_t new_position);

    struct ReadResult {
        enum Status { kInlined, kLookupDB, kFailed };
        Status                     status;
        uint64_t                   localid{0};
        LogData                    data;
        protocol::SharedLogMessage original_request;
    };
    using ReadResultVec = absl::InlinedVector<ReadResult, 4>;
    void PollReadResults(ReadResultVec* results);

    std::optional<IndexDataProto> PollIndexData();
    std::optional<std::vector<uint32_t>> GrabShardProgressForSending();

    struct TrimOp {
        uint32_t user_logspace;
        uint64_t trim_seqnum;
    };
    using TrimOpVec = absl::InlinedVector<TrimOp, 4>;
    TrimOpVec FetchTrimOps() const;
    void MarkTrimOpFinished(const TrimOp& trim_op);

private:
    const View::Storage* storage_node_;

    bool shard_progrss_dirty_;
    absl::flat_hash_map</* engine_id */ uint16_t,
                        /* localid */ uint32_t> shard_progrsses_;

    utils::SimpleObjectPool<Entry> entry_pool_;

    uint64_t persisted_seqnum_position_;
    std::deque<uint64_t> live_seqnums_;
    absl::flat_hash_map</* seqnum */ uint64_t, const Entry*> live_log_entries_;
    LogEntryVec entries_for_persistence_;

    absl::flat_hash_map</* localid */ uint64_t, Entry*> pending_log_entries_;

    std::multimap</* seqnum */ uint64_t,
                  protocol::SharedLogMessage> pending_read_requests_;
    ReadResultVec pending_read_results_;

    IndexDataProto index_data_;

    absl::flat_hash_map</* user_logspace */ uint32_t,
                        /* trim_seqnum */ uint64_t> trim_seqnums_;

    stat::StatisticsCollector<int> journal_delay_stat_;
    stat::StatisticsCollector<int> live_entries_stat_;

    void OnNewLogs(uint32_t metalog_seqnum,
                   uint64_t start_seqnum, uint64_t start_localid,
                   uint32_t delta) override;
    void OnTrim(uint32_t metalog_seqnum,
                const MetaLogProto::TrimProto& trim_proto) override;
    void OnFinalized(uint32_t metalog_position) override;

    void AdvanceShardProgress(uint16_t engine_id);
    void ShrinkLiveEntriesIfNeeded();

    static void ClearLogData(LogData* data);

    DISALLOW_COPY_AND_ASSIGN(LogStorage);
};

}  // namespace log
}  // namespace faas
