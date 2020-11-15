#include "log/engine_core.h"

#include "common/flags.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

#define HLOG(l) LOG(l) << "LogEngineCore: "
#define HVLOG(l) VLOG(l) << "LogEngineCore: "

namespace faas {
namespace log {

EngineCore::EngineCore(uint16_t my_node_id)
    : my_node_id_(my_node_id),
      log_counter_(0) {
    fsm_.SetNewViewCallback(absl::bind_front(&EngineCore::OnFsmNewView, this));
    fsm_.SetLogReplicatedCallback(absl::bind_front(&EngineCore::OnFsmLogReplicated, this));
}

EngineCore::~EngineCore() {}

void EngineCore::SetLogPersistedCallback(LogPersistedCallback cb) {
    log_persisted_cb_ = cb;
}

void EngineCore::SetLogDiscardedCallback(LogDiscardedCallback cb) {
    log_discarded_cb_ = cb;
}

void EngineCore::SetAppendBackupLogCallback(AppendBackupLogCallback cb) {
    append_backup_log_cb_ = cb;
}

void EngineCore::SetSendLocalCutMessageCallback(SendLocalCutMessageCallback cb) {
    send_local_cut_message_cb_ = cb;
}

void EngineCore::NewFsmRecordsMessage(const FsmRecordsMsgProto& message) {
    for (const FsmRecordProto& record : message.records()) {
        fsm_.OnRecvRecord(record);
    }
}

bool EngineCore::NewLocalLog(uint32_t log_tag, std::span<const char> data, uint64_t* log_localid) {
    const Fsm::View* view = fsm_.current_view();
    if (view == nullptr) {
        HLOG(ERROR) << "No view message from sequencer!";
        return false;
    }
    if (!view->has_node(my_node_id_)) {
        HLOG(ERROR) << "Current view does not contain myself!";
        return false;
    }
    LogEntry* log_entry = new LogEntry;
    log_entry->seqnum = 0;
    log_entry->tag = log_tag;
    log_entry->data = std::string(data.data(), data.size());
    log_entry->localid = BuildLocalId(view->id(), my_node_id_, log_counter_++);
    pending_entries_[log_entry->localid] = std::unique_ptr<LogEntry>(log_entry);

    view->ForEachBackupNode(my_node_id_, [this, view, log_entry] (uint16_t node_id) {
        append_backup_log_cb_(view->id(), node_id, log_entry);
    });
    *log_localid = log_entry->localid;
    return true;
}

void EngineCore::NewRemoteLog(uint64_t log_localid, uint32_t log_tag, std::span<const char> data) {
    std::unique_ptr<LogEntry> log_entry(new LogEntry);
    log_entry->localid = log_localid;
    log_entry->seqnum = 0;
    log_entry->tag = log_tag;
    log_entry->data = std::string(data.data(), data.size());
    uint16_t view_id = LocalIdToViewId(log_localid);
    uint16_t node_id = LocalIdToNodeId(log_localid);
    if (node_id == my_node_id_) {
        HLOG(FATAL) << "Same node_id from remote logs";
    }
    const Fsm::View* current_view = fsm_.current_view();
    if (current_view != nullptr && current_view->id() > view_id) {
        // Can safely discard this log
        return;
    }
    pending_entries_[log_localid] = std::move(log_entry);
    if (current_view != nullptr && current_view->id() == view_id) {
        AdvanceLogProgress(current_view, node_id);
    }
}

void EngineCore::OnFsmNewView(const Fsm::View* view) {
    auto iter = pending_entries_.begin();
    while (iter != pending_entries_.end()) {
        if (LocalIdToViewId(iter->first) >= view->id()) {
            break;
        }
        std::unique_ptr<LogEntry> log_entry = std::move(iter->second);
        iter = pending_entries_.erase(iter);
        log_discarded_cb_(std::move(log_entry));
    }
    log_progress_.clear();
    for (size_t i = 0; i < view->num_nodes(); i++) {
        uint16_t node_id = view->node(i);
        if (node_id != my_node_id_) {
            log_progress_[node_id] = 0;
            AdvanceLogProgress(view, node_id);
        }
    }
}

void EngineCore::OnFsmLogReplicated(uint64_t start_localid, uint64_t start_seqnum,
                                    uint32_t delta) {
    for (uint32_t i = 0; i < delta; i++) {
        uint64_t localid = start_localid + i;
        auto iter = pending_entries_.find(localid);
        if (iter == pending_entries_.end()) {
            continue;
        }
        std::unique_ptr<LogEntry> log_entry = std::move(iter->second);
        pending_entries_.erase(iter);
        log_entry->seqnum = start_seqnum + i;
        log_persisted_cb_(std::move(log_entry));
    }
}

void EngineCore::AdvanceLogProgress(const Fsm::View* view, uint16_t node_id) {
    DCHECK(view->has_node(node_id));
    uint32_t counter = log_progress_[node_id];
    while (pending_entries_.count(BuildLocalId(view->id(), node_id, counter)) > 0) {
        counter++;
    }
    log_progress_[node_id] = counter;
}

}  // namespace log
}  // namespace faas
