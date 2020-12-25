#include "log/engine_core.h"

#include "common/time.h"
#include "utils/random.h"
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

void EngineCore::OnNewFsmRecordsMessage(const FsmRecordsMsgProto& message) {
    for (const FsmRecordProto& record : message.records()) {
        fsm_.OnRecvRecord(record);
    }
}

bool EngineCore::LogTagToPrimaryNode(uint64_t tag, uint16_t* primary_node_id) {
    const Fsm::View* current_view = fsm_.current_view();
    if (current_view == nullptr) {
        HLOG(ERROR) << "No view message from sequencer!";
        return false;
    }
    if (tag == kEmptyLogTag) {
        if (current_view->has_node(my_node_id_)) {
            *primary_node_id = my_node_id_;
        } else {
            HLOG(WARNING) << "Current view does not contain myself, "
                          << "will choose a random node for this log";
            *primary_node_id = current_view->PickOneNode();
        }
    } else {
        *primary_node_id = current_view->LogTagToPrimaryNode(tag);
    }
    return true;
}

EngineCore::LogEntry* EngineCore::AllocLogEntry(uint64_t tag, uint64_t localid,
                                                std::span<const char> data) {
    LogEntry* log_entry = log_entry_pool_.Get();
    log_entry->localid = localid;
    log_entry->seqnum = 0;
    log_entry->tag = tag;
    log_entry->data.Reset();
    if (data.size() > 0) {
        log_entry->data.AppendData(data);
    }
    return log_entry;
}

bool EngineCore::StoreLogAsPrimaryNode(uint64_t tag, std::span<const char> data,
                                       uint64_t* localid) {
    const Fsm::View* current_view = fsm_.current_view();
    if (current_view == nullptr) {
        HLOG(ERROR) << "No view message from sequencer!";
        return false;
    }
    if (!current_view->has_node(my_node_id_)) {
        HLOG(ERROR) << "Current view does not contain myself!";
        return false;
    }
    if (tag != kEmptyLogTag && my_node_id_ != current_view->LogTagToPrimaryNode(tag)) {
        HLOG(ERROR) << fmt::format("This node is not the primary node of log tag {} "
                                   "in the current view", tag, current_view->id());
        return false;
    }
    HVLOG(1) << fmt::format("NewLocalLog: tag={}, data_size={}", tag, data.size());
    LogEntry* log_entry = AllocLogEntry(
        tag,
        /* localid= */ BuildLocalId(current_view->id(), my_node_id_, next_localid_++),
        data);
    pending_entries_[log_entry->localid] = log_entry;
    ScheduleLocalCutIfNecessary();
    *localid = log_entry->localid;
    return true;
}

bool EngineCore::StoreLogAsBackupNode(uint64_t tag, std::span<const char> data,
                                      uint64_t localid) {
    uint16_t view_id = LocalIdToViewId(localid);
    uint16_t primary_node_id = LocalIdToNodeId(localid);
    if (primary_node_id == my_node_id_) {
        HLOG(FATAL) << "Primary node ID is same as mine";
    }
    HVLOG(1) << fmt::format("Store new log as backup node (view_id={}, primary_node_id={})",
                            view_id, primary_node_id);
    const Fsm::View* current_view = fsm_.current_view();
    if (current_view != nullptr && current_view->id() > view_id) {
        // Can safely discard this log
        HLOG(WARNING) << "Receive outdated log";
        return false;
    }
    pending_entries_[localid] = AllocLogEntry(tag, localid, data);
    if (current_view != nullptr && current_view->id() == view_id) {
        AdvanceLogProgress(current_view, primary_node_id);
    }
    return true;
}

void EngineCore::AddWaitForReplication(uint64_t tag, uint64_t localid) {
    pending_entries_[localid] = AllocLogEntry(tag, localid, std::span<const char>());
}

void EngineCore::OnFsmNewView(const Fsm::View* view) {
    auto iter = pending_entries_.begin();
    while (iter != pending_entries_.end()) {
        if (LocalIdToViewId(iter->first) >= view->id()) {
            break;
        }
        LogEntry* log_entry = iter->second;
        iter = pending_entries_.erase(iter);
        log_discarded_cb_(log_entry->localid);
        log_entry_pool_.Return(log_entry);
    }
    next_localid_ = 0;
    log_progress_.clear();
    if (view->has_node(my_node_id_)) {
        view->ForEachPrimaryNode(my_node_id_, [this, view] (uint16_t node_id) {
            log_progress_[node_id] = 0;
            AdvanceLogProgress(view, node_id);
        });
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
        HVLOG(1) << fmt::format("Log (localid {:#018x}) replicated with seqnum {:#018x}",
                                localid, start_seqnum + i);
        LogEntry* log_entry = iter->second;
        pending_entries_.erase(iter);
        log_entry->seqnum = start_seqnum + i;
        log_persisted_cb_(log_entry->localid, log_entry->seqnum);
        persisted_entries_[log_entry->seqnum] = log_entry;
    }
}

void EngineCore::AdvanceLogProgress(const Fsm::View* view, uint16_t node_id) {
    DCHECK(view->has_node(node_id));
    if (!log_progress_.contains(node_id)) {
        HLOG(ERROR) << fmt::format("This node is not backup of node {} in the view {}",
                                   node_id, view->id());
        return;
    }
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

void EngineCore::DoStateCheck(std::ostringstream& stream) const {
    stream << fmt::format("My NodeId: {}\n", my_node_id_);
    fsm_.DoStateCheck(stream);
    if (!pending_entries_.empty()) {
        stream << fmt::format("There are {} pending log entries\n",
                              pending_entries_.size());
        int counter = 0;
        for (const auto& entry : pending_entries_) {
            uint64_t localid = entry.first;
            const LogEntry* log_entry = entry.second;
            counter++;
            stream << fmt::format("--[{}] LocalId={:#018x} Tag={}",
                                  counter, localid, log_entry->tag);
            uint16_t node_id = LocalIdToNodeId(localid);
            if (node_id == my_node_id_) {
                stream << " SrcNode=myself";
            } else {
                stream << " SrcNode=" << node_id;
            }
            stream << "\n";
            if (counter >= 32) {
                stream << "...more...\n";
                break;
            }
        }
    }
    stream << "LogProcess:";
    for (const auto& entry : log_progress_) {
        stream << fmt::format(" Node[{}]={:#010x}", entry.first, entry.second);
    }
    stream << fmt::format(" Myself={:#010x}", next_localid_);
    stream << "\n";
}

}  // namespace log
}  // namespace faas
