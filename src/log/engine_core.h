#pragma once

#include "log/common.h"
#include "log/fsm.h"
#include "log/storage.h"

namespace faas {
namespace log {

class EngineCore {
public:
    explicit EngineCore(uint16_t my_node_id);
    ~EngineCore();

    const Fsm* fsm() const { return &fsm_; }

    typedef std::function<void(std::unique_ptr<LogEntry> /* log_entry */)>
            LogPersistedCallback;
    void SetLogPersistedCallback(LogPersistedCallback cb);

    typedef std::function<void(std::unique_ptr<LogEntry> /* log_entry */)>
            LogDiscardedCallback;
    void SetLogDiscardedCallback(LogDiscardedCallback cb);

    typedef std::function<void(int /* duration_us */)> ScheduleLocalCutCallback;
    void SetScheduleLocalCutCallback(ScheduleLocalCutCallback cb);

    void BuildLocalCutMessage(LocalCutMsgProto* message);
    void OnNewFsmRecordsMessage(const FsmRecordsMsgProto& message);

    bool LogTagToPrimaryNode(uint64_t tag, uint16_t* primary_node_id);
    bool StoreLogAsPrimaryNode(uint64_t tag, std::span<const char> data, uint64_t* localid);
    bool StoreLogAsBackupNode(uint64_t tag, std::span<const char> data, uint64_t localid);
    void AddWaitForReplication(uint64_t tag, uint64_t localid);

    void DoStateCheck(std::ostringstream& stream) const;

private:
    Fsm fsm_;
    uint16_t my_node_id_;
    int local_cut_interval_us_;

    LogPersistedCallback      log_persisted_cb_;
    LogDiscardedCallback      log_discarded_cb_;
    ScheduleLocalCutCallback  schedule_local_cut_cb_;

    uint32_t next_localid_;
    std::map</* localid */ uint64_t, std::unique_ptr<LogEntry>> pending_entries_;
    absl::flat_hash_map</* node_id */ uint16_t, uint32_t> log_progress_;

    bool local_cut_scheduled_;
    int64_t last_local_cut_timestamp_;

    void OnFsmNewView(const Fsm::View* view);
    void OnFsmLogReplicated(uint64_t start_localid, uint64_t start_seqnum, uint32_t delta);

    void AdvanceLogProgress(const Fsm::View* view, uint16_t node_id);
    void ScheduleLocalCutIfNecessary();

    DISALLOW_COPY_AND_ASSIGN(EngineCore);
};

}  // namespace log
}  // namespace faas
