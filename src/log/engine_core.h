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

    typedef std::function<void(uint16_t /* view_id */, uint16_t /* backup_node_id */,
                               const LogEntry* /* log_entry */)>
            AppendBackupLogCallback;
    void SetAppendBackupLogCallback(AppendBackupLogCallback cb);

    typedef std::function<void(std::span<const char> /* data */)>
            SendLocalCutMessageCallback;
    void SetSendLocalCutMessageCallback(SendLocalCutMessageCallback cb);

    int local_cut_interval_us() const;
    void MarkAndSendLocalCut();

    void NewFsmRecordsMessage(const FsmRecordsMsgProto& message);
    bool NewLocalLog(uint32_t log_tag, std::span<const char> data, uint64_t* log_localid);
    void NewRemoteLog(uint64_t log_localid, uint32_t log_tag, std::span<const char> data);

private:
    Fsm fsm_;
    uint16_t my_node_id_;

    LogPersistedCallback         log_persisted_cb_;
    LogDiscardedCallback         log_discarded_cb_;
    AppendBackupLogCallback      append_backup_log_cb_;
    SendLocalCutMessageCallback  send_local_cut_message_cb_;

    uint32_t log_counter_;
    std::map</* localid */ uint64_t, std::unique_ptr<LogEntry>> pending_entries_;
    absl::flat_hash_map</* node_id */ uint16_t, uint32_t> log_progress_;

    void OnFsmNewView(const Fsm::View* view);
    void OnFsmLogReplicated(uint64_t start_localid, uint64_t start_seqnum, uint32_t delta);

    void AdvanceLogProgress(const Fsm::View* view, uint16_t node_id);

    DISALLOW_COPY_AND_ASSIGN(EngineCore);
};

}  // namespace log
}  // namespace faas
