#pragma once

#include "utils/object_pool.h"
#include "log/common.h"
#include "log/fsm.h"
#include "log/storage.h"
#include "log/tag_index.h"

namespace faas {
namespace log {

class EngineCore {
public:
    explicit EngineCore(uint16_t my_node_id);
    ~EngineCore();

    static absl::Duration local_cut_interval();

    const Fsm* fsm() const { return &fsm_; }
    uint32_t fsm_progress() const { return fsm_.progress(); }

    typedef std::function<void(uint64_t /* localid */, uint64_t /* seqnum */)>
            LogPersistedCallback;
    void SetLogPersistedCallback(LogPersistedCallback cb);

    typedef std::function<void(uint64_t /* localid */)>
            LogDiscardedCallback;
    void SetLogDiscardedCallback(LogDiscardedCallback cb);

    bool BuildLocalCutMessage(LocalCutMsgProto* message);
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

    LogPersistedCallback  log_persisted_cb_;
    LogDiscardedCallback  log_discarded_cb_;

    uint32_t next_localid_;

    struct LogEntry {
        uint64_t localid;
        uint64_t seqnum;
        uint64_t tag;
        utils::AppendableBuffer data;
    };
    utils::SimpleObjectPool<LogEntry> log_entry_pool_;
    std::map</* localid */ uint64_t, LogEntry*> pending_entries_;
    std::map</* seqnum */ uint64_t, LogEntry*> persisted_entries_;

    absl::flat_hash_map</* node_id */ uint16_t, uint32_t> log_progress_;

    bool log_progress_dirty_;

    LogEntry* AllocLogEntry(uint64_t tag, uint64_t localid, std::span<const char> data);

    void OnFsmNewView(const Fsm::View* view);
    void OnFsmLogReplicated(uint64_t start_localid, uint64_t start_seqnum, uint32_t delta);

    void AdvanceLogProgress(const Fsm::View* view, uint16_t node_id);
    void ScheduleLocalCutIfNecessary();

    DISALLOW_COPY_AND_ASSIGN(EngineCore);
};

}  // namespace log
}  // namespace faas
