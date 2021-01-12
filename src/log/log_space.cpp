#include "log/log_space.h"

#include "log/flags.h"

namespace faas {
namespace log {

MetaLogPrimary::MetaLogPrimary(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      sequencer_node_(view->GetSequencerNode(sequencer_id)) {
    log_header_ = fmt::format("MetaLogPrimary[{}]: ", view->id());
    state_ = kNormal;
}

MetaLogPrimary::~MetaLogPrimary() {}

MetaLogBackup::MetaLogBackup(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id) {
    log_header_ = fmt::format("MetaLogBackup[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

MetaLogBackup::~MetaLogBackup() {}

LogProducer::LogProducer(uint16_t engine_id, const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kLiteMode, view, sequencer_id) {
    AddInterestedShard(engine_id);
    log_header_ = fmt::format("LogProducer[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

LogProducer::~LogProducer() {}

LogStorage::LogStorage(uint16_t storage_id, const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kLiteMode, view, sequencer_id),
      storage_node_(view_->GetStorageNode(storage_id)),
      shard_progrss_dirty_(false),
      persisted_seqnum_position_(0) {
    for (uint16_t engine_id : storage_node_->GetSourceEngineNodes()) {
        AddInterestedShard(engine_id);
        shard_progrsses_[engine_id] = 0;
    }
    log_header_ = fmt::format("LogStorage[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

LogStorage::~LogStorage() {}

bool LogStorage::Store(const LogMetaData& log_metadata,
                       std::span<const char> log_data) {
    uint64_t localid = log_metadata.localid;
    uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
    if (!storage_node_->IsSourceEngineNode(engine_id)) {
        HLOG(ERROR) << fmt::format("Not storage node (node_id {}) for engine (node_id {})",
                                   storage_node_->node_id(), engine_id);
        return false;
    }
    pending_log_entries_[localid].reset(new LogEntry {
        .metadata = log_metadata,
        .data = std::string(log_data.data(), log_data.size()),
    });
    AdvanceShardProgress(engine_id);
    return true;
}

void LogStorage::ReadAt(const protocol::SharedLogMessage& request) {
    uint32_t seqnum = request.seqnum;
    if (seqnum >= seqnum_position()) {
        pending_read_requests_.insert(std::make_pair(seqnum, request));
        return;
    }
    ReadResult result = {
        .status = ReadResult::kFailed,
        .log_entry = nullptr,
        .original_request = request
    };
    if (live_log_entries_.contains(seqnum)) {
        result.status = ReadResult::kOK;
        result.log_entry = live_log_entries_[seqnum];
    } else if (seqnum < persisted_seqnum_position_) {
        result.status = ReadResult::kLookupDB;
    }
    pending_read_results_.push_back(std::move(result));
}

bool LogStorage::GrabLogEntriesForPersistence(
        std::vector<std::shared_ptr<const LogEntry>>* log_entries,
        uint32_t* new_position) {
    size_t idx = absl::c_lower_bound(live_seqnums_,
                                     persisted_seqnum_position_)
                 - live_seqnums_.begin();
    if (idx >= live_seqnums_.size()) {
        return false;
    }
    log_entries->clear();
    for (size_t i = idx; i < live_seqnums_.size(); i++) {
        uint32_t seqnum = live_seqnums_[i];
        DCHECK(live_log_entries_.contains(seqnum));
        log_entries->push_back(live_log_entries_[seqnum]);
    }
    DCHECK(!log_entries->empty());
    *new_position = live_seqnums_.back() + 1;
    return true;
}

void LogStorage::LogEntriesPersisted(uint32_t new_position) {
    persisted_seqnum_position_ = new_position;
    ShrinkLiveEntriesIfNeeded();
}

void LogStorage::PollReadResults(ReadResultVec* results) {
    *results = std::move(pending_read_results_);
    pending_read_results_.clear();
}

bool LogStorage::GrabShardProgressForSending(std::vector<uint32_t>* progress) {
    if (!shard_progrss_dirty_) {
        return false;
    }
    progress->clear();
    for (uint16_t engine_id : storage_node_->GetSourceEngineNodes()) {
        progress->push_back(shard_progrsses_[engine_id]);
    }
    shard_progrss_dirty_ = false;
    return true;
}

void LogStorage::OnNewLogs(uint32_t start_seqnum, uint64_t start_localid,
                           uint32_t delta) {
    auto iter = pending_read_requests_.begin();
    while (iter != pending_read_requests_.end()) {
        const protocol::SharedLogMessage& request = iter->second; 
        if (request.seqnum >= start_seqnum) {
            break;
        }
        pending_read_results_.push_back(ReadResult {
            .status = ReadResult::kFailed,
            .log_entry = nullptr,
            .original_request = request
        });
        iter = pending_read_requests_.erase(iter);
    }
    for (size_t i = 0; i < delta; i++) {
        uint32_t seqnum = start_seqnum + i;
        uint64_t localid = start_localid + i;
        if (!pending_log_entries_.contains(localid)) {
            HLOG(FATAL) << fmt::format("Cannot find pending log entry for localid {}",
                                       bits::HexStr0x(localid));
        }
        // Build the log entry for live_log_entries_
        LogEntry* log_entry = pending_log_entries_[localid].release();
        pending_log_entries_.erase(localid);
        log_entry->metadata.seqnum = seqnum;
        std::shared_ptr<const LogEntry> log_entry_ptr(log_entry);
        // Update live_seqnums_ and live_log_entries_
        DCHECK(live_seqnums_.empty() || seqnum > live_seqnums_.back());
        live_seqnums_.push_back(seqnum);
        live_log_entries_[seqnum] = log_entry_ptr;
        DCHECK_EQ(live_seqnums_.size(), live_log_entries_.size());
        ShrinkLiveEntriesIfNeeded();
        // Check if we have read request on it
        if (iter != pending_read_requests_.end() && iter->second.seqnum == seqnum) {
            pending_read_results_.push_back(ReadResult {
                .status = ReadResult::kOK,
                .log_entry = log_entry_ptr,
                .original_request = iter->second
            });
            iter = pending_read_requests_.erase(iter);
        }
    }
}

void LogStorage::OnFinalized() {
    if (!pending_log_entries_.empty()) {
        HLOG(WARNING) << fmt::format("{} pending log entries discarded",
                                     pending_log_entries_.size());
        pending_log_entries_.clear();
    }
}

void LogStorage::AdvanceShardProgress(uint16_t engine_id) {
    uint32_t current = shard_progrsses_[engine_id];
    while (pending_log_entries_.contains(bits::JoinTwo32(engine_id, current))) {
        current++;
    }
    if (current > shard_progrsses_[engine_id]) {
        shard_progrss_dirty_ = true;
        shard_progrsses_[engine_id] = current;
    }
}

void LogStorage::ShrinkLiveEntriesIfNeeded() {
    size_t max_size = absl::GetFlag(FLAGS_slog_storage_max_live_entries);
    while (live_seqnums_.size() > max_size
             && live_seqnums_.front() < persisted_seqnum_position_) {
        live_log_entries_.erase(live_seqnums_.front());
        live_seqnums_.pop_front();
        DCHECK_EQ(live_seqnums_.size(), live_log_entries_.size());
    }
}

}  // namespace log
}  // namespace faas
