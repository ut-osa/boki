#include "log/core.h"

#include "common/flags.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

#define HLOG(l) LOG(l) << "LogCore: "
#define HVLOG(l) VLOG(l) << "LogCore: "

namespace faas {
namespace log {

Core::Core(uint16_t my_node_id)
    : my_node_id_(my_node_id),
      num_replicas_(absl::GetFlag(FLAGS_shared_log_num_replicas)),
      log_counter_(0) {
}

Core::~Core() {
}

void Core::SetLogReplicatedCallback(LogReplicatedCallback cb) {
    log_replicated_cb_ = cb;
}

void Core::SetLogDiscardedCallback(LogDiscardedCallback cb) {
    log_discarded_cb_ = cb;
}

void Core::SetAppendBackupLogCallback(AppendBackupLogCallback cb) {
    append_backup_log_cb_ = cb;
}

void Core::SetSendSequencerMessageCallback(SendSequencerMessageCallback cb) {
    send_sequencer_message_cb_ = cb;
}

void Core::OnSequencerMessage(int seqnum, std::span<const char> data) {
    DCHECK_GE(seqnum, 0);
}

void Core::ApplySequencerMessage(std::span<const char> data) {
    json message;
    try {
        message = json::parse(data);
    } catch (const json::parse_error& e) {
        HLOG(FATAL) << "Failed to parse json: " << e.what();
    }

}

const View* Core::GetView(uint16_t view_id) const {
    absl::ReaderMutexLock lk(&mu_);
    if (views_.contains(view_id)) {
        return views_.at(view_id).get();
    } else {
        return nullptr;
    }
}

bool Core::NewLocalLog(uint32_t log_tag, std::span<const char> data, uint64_t* log_localid) {
    std::unique_ptr<LogEntry> log_entry(new LogEntry);
    log_entry->seqnum = 0;
    log_entry->tag = log_tag;
    log_entry->data = std::string(data.data(), data.size());
    View* view = nullptr;
    uint64_t localid = 0;
    {
        absl::MutexLock lk(&mu_);
        if (current_view_ == nullptr) {
            HLOG(ERROR) << "No view message from sequencer!";
            return false;
        }
        view = current_view_;
        if (!view->has_node(my_node_id_)) {
            HLOG(ERROR) << "Current view does not contain myself!";
            return false;
        }
        localid = BuildLocalId(view->id(), my_node_id_, log_counter_++);
        log_entry->localid = localid;
        pending_entries_[localid] = std::move(log_entry);
        *log_localid = localid;
    }
    for (int i = 1; i < num_replicas_; i++) {
        uint16_t node_id = view->get_replica_node(my_node_id_, gsl::narrow_cast<size_t>(i));
        append_backup_log_cb_(view->id(), node_id, localid, log_tag, data);
    }
    return true;
}

void Core::NewRemoteLog(uint64_t log_localid, uint32_t log_tag, std::span<const char> data) {
    std::unique_ptr<LogEntry> log_entry(new LogEntry);
    log_entry->localid = log_localid;
    log_entry->seqnum = 0;
    log_entry->tag = log_tag;
    log_entry->data = std::string(data.data(), data.size());
    uint16_t view_id = LocalIdToViewId(log_localid);
    uint16_t node_id = LocalIdToNodeId(log_localid);
    DCHECK(node_id != my_node_id_);
    {
        absl::MutexLock lk(&mu_);
        if (current_view_ != nullptr && current_view_->id() > view_id) {
            // Can safely discard this log
            return;
        }
        pending_entries_[log_localid] = std::move(log_entry);
        if (current_view_ != nullptr && current_view_->id() == view_id) {
            AdvanceLogProgress(node_id);
        }
    }
}

bool Core::ReadLog(uint64_t log_seqnum, std::span<const char>* data) {
    return storage_->Read(log_seqnum, data);
}

void Core::AdvanceLogProgress(uint16_t node_id) {
    DCHECK(current_view_ != nullptr);
    DCHECK(current_view_->has_node(node_id));
    uint32_t counter = log_progress_[node_id];
    while (pending_entries_.contains(BuildLocalId(current_view_->id(), node_id, counter))) {
        counter++;
    }
    log_progress_[node_id] = counter;
}

}  // namespace log
}  // namespace faas
