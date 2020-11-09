#pragma once

#include "log/common.h"
#include "log/view.h"
#include "log/storage.h"

namespace faas {
namespace log {

class Core {
public:
    explicit Core(uint16_t my_node_id);
    ~Core();

    typedef std::function<void(std::pair<uint64_t, uint64_t> /* localid_range */,
                               std::pair<uint64_t, uint64_t> /* seqnum_range */)>
            LogReplicatedCallback;
    void SetLogReplicatedCallback(LogReplicatedCallback cb);

    typedef std::function<void(uint64_t /* localid */)> LogDiscardedCallback;
    void SetLogDiscardedCallback(LogDiscardedCallback cb);

    typedef std::function<void(uint16_t /* view_id */, uint16_t /* backup_node_id */,
                               uint64_t /* log_localid */, uint32_t /* log_tag */,
                               std::span<const char> /* data */)>
            AppendBackupLogCallback;
    void SetAppendBackupLogCallback(AppendBackupLogCallback cb);

    typedef std::function<void(std::span<const char> /* data */)>
            SendSequencerMessageCallback;
    void SetSendSequencerMessageCallback(SendSequencerMessageCallback cb);

    void OnSequencerMessage(int seqnum, std::span<const char> data);
    const View* GetView(uint16_t view_id) const;

    bool NewLocalLog(uint32_t log_tag, std::span<const char> data, uint64_t* log_localid);
    void NewRemoteLog(uint64_t log_localid, uint32_t log_tag, std::span<const char> data);

    bool ReadLog(uint64_t log_seqnum, std::span<const char>* data);

private:
    uint16_t my_node_id_;
    int num_replicas_;

    LogReplicatedCallback        log_replicated_cb_;
    LogDiscardedCallback         log_discarded_cb_;
    AppendBackupLogCallback      append_backup_log_cb_;
    SendSequencerMessageCallback send_sequencer_message_cb_;

    mutable absl::Mutex mu_;

    uint32_t log_counter_  ABSL_GUARDED_BY(mu_);
    View* current_view_    ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* view_id */ uint16_t, std::unique_ptr<View>>
        views_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* localid */ uint64_t, std::unique_ptr<LogEntry>>
        pending_entries_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* node_id */ uint16_t, uint32_t>
        log_progress_ ABSL_GUARDED_BY(mu_);

    std::unique_ptr<Storage> storage_;

    void ApplySequencerMessage(std::span<const char> data);
    void AdvanceLogProgress(uint16_t node_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

    DISALLOW_COPY_AND_ASSIGN(Core);
};

}  // namespace log
}  // namespace faas
