#include "log/engine_core.h"

#include "common/time.h"
#include "log/flags.h"

#define log_header_ "LogEngineCore: "

namespace faas {
namespace log {

EngineCore::EngineCore(uint16_t my_node_id)
    : my_node_id_(my_node_id),
      local_cut_interval_us_(absl::GetFlag(FLAGS_slog_local_cut_interval_us)),
      next_localid_(0),
      local_cut_scheduled_(false),
      last_local_cut_timestamp_(0) {
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

void EngineCore::SetScheduleLocalCutCallback(ScheduleLocalCutCallback cb) {
    schedule_local_cut_cb_ = cb;
}

void EngineCore::BuildLocalCutMessage(LocalCutMsgProto* message) {
    local_cut_scheduled_ = false;
    last_local_cut_timestamp_ = GetMonotonicMicroTimestamp();
    const Fsm::View* view = fsm_.current_view();
    message->Clear();
    message->set_view_id(view->id());
    message->set_my_node_id(my_node_id_);
    message->add_localid_cuts(next_localid_);
    view->ForEachPrimaryNode(my_node_id_, [this, message] (uint16_t node_id) {
        message->add_localid_cuts(log_progress_[node_id]);
    });
}

void EngineCore::NewFsmRecordsMessage(const FsmRecordsMsgProto& message) {
    for (const FsmRecordProto& record : message.records()) {
        fsm_.OnRecvRecord(record);
    }
}

bool EngineCore::NewLocalLog(uint32_t tag, std::span<const char> data,
                             const Fsm::View** view, uint64_t* localid) {
    const Fsm::View* current_view = fsm_.current_view();
    if (current_view == nullptr) {
        HLOG(ERROR) << "No view message from sequencer!";
        return false;
    }
    if (!current_view->has_node(my_node_id_)) {
        HLOG(ERROR) << "Current view does not contain myself!";
        return false;
    }
    HVLOG(1) << fmt::format("NewLocalLog: tag={}, data_size={}", tag, data.size());
    LogEntry* log_entry = new LogEntry;
    log_entry->seqnum = 0;
    log_entry->tag = tag;
    log_entry->data = std::string(data.data(), data.size());
    log_entry->localid = BuildLocalId(current_view->id(), my_node_id_, next_localid_++);
    pending_entries_[log_entry->localid] = std::unique_ptr<LogEntry>(log_entry);
    ScheduleLocalCutIfNecessary();

    *view = current_view;
    *localid = log_entry->localid;
    return true;
}

void EngineCore::NewRemoteLog(uint64_t localid, uint32_t tag, std::span<const char> data) {
    std::unique_ptr<LogEntry> log_entry(new LogEntry);
    log_entry->localid = localid;
    log_entry->seqnum = 0;
    log_entry->tag = tag;
    log_entry->data = std::string(data.data(), data.size());
    uint16_t view_id = LocalIdToViewId(localid);
    uint16_t node_id = LocalIdToNodeId(localid);
    if (node_id == my_node_id_) {
        HLOG(FATAL) << "Same node_id from remote logs";
    }
    HVLOG(1) << fmt::format("Receive remote log (view_id={}, node_id={})", view_id, node_id);
    const Fsm::View* current_view = fsm_.current_view();
    if (current_view != nullptr && current_view->id() > view_id) {
        // Can safely discard this log
        HLOG(WARNING) << "Receive outdated log";
        return;
    }
    pending_entries_[localid] = std::move(log_entry);
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
    next_localid_ = 0;
    log_progress_.clear();
    view->ForEachPrimaryNode(my_node_id_, [this, view] (uint16_t node_id) {
        log_progress_[node_id] = 0;
        AdvanceLogProgress(view, node_id);
    });
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
    uint32_t initial_counter = counter;
    while (pending_entries_.count(BuildLocalId(view->id(), node_id, counter)) > 0) {
        counter++;
    }
    if (counter > initial_counter) {
        log_progress_[node_id] = counter;
        ScheduleLocalCutIfNecessary();
    }
}

void EngineCore::ScheduleLocalCutIfNecessary() {
    if (local_cut_scheduled_) {
        return;
    }
    if (last_local_cut_timestamp_ == 0) {
        last_local_cut_timestamp_ = GetMonotonicMicroTimestamp();
    }
    int64_t next_local_cut_timestamp = last_local_cut_timestamp_ + local_cut_interval_us_;
    int64_t duration = next_local_cut_timestamp - GetMonotonicMicroTimestamp();
    if (duration < 0) {
        schedule_local_cut_cb_(0);
    } else {
        schedule_local_cut_cb_(gsl::narrow_cast<int>(duration));
    }
    local_cut_scheduled_ = true;
}

}  // namespace log
}  // namespace faas
