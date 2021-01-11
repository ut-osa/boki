#include "log/log_space.h"

#include "log/flags.h"

namespace faas {
namespace log {

LogSpaceBase::LogSpaceBase(Mode mode, const View* view, uint16_t sequencer_id)
    : mode_(mode),
      state_(kCreated),
      view_(view),
      sequencer_id_(sequencer_id),
      metalog_position_(0),
      seqnum_position_(0),
      log_header_(fmt::format("LogSpace[{}-{}]: ", view->id(), sequencer_id)),
      shard_progrsses_(view->num_engine_nodes(), 0) {}

void LogSpaceBase::AddInterestedShard(uint16_t engine_id) {
    DCHECK(state_ == kCreated);
    const View::NodeIdVec& engine_node_ids = view_->GetEngineNodes();
    size_t idx = absl::c_find(engine_node_ids, engine_id) - engine_node_ids.begin();
    DCHECK_LT(idx, engine_node_ids.size());
    interested_shards_.insert(idx);
}

bool LogSpaceBase::ProvideMetaLog(const MetaLogProto& meta_log) {
    DCHECK(state_ == kNormal);
    if (mode_ == kLiteMode && meta_log.type() == MetaLogProto::TRIM) {
        HLOG(WARNING) << fmt::format("Trim log (seqnum={}) is simply ignore in lite mode",
                                     meta_log.metalog_seqnum());
        return false;
    }
    uint32_t seqnum = meta_log.metalog_seqnum();
    MetaLogProto* meta_log_copy = metalog_pool_.Get();
    meta_log_copy->CopyFrom(meta_log);
    pending_metalogs_[seqnum] = meta_log_copy;
    uint32_t prev_metalog_position = metalog_position_;
    AdvanceMetaLogProgress();
    return metalog_position_ > prev_metalog_position;
}

void LogSpaceBase::Freeze() {
    DCHECK(state_ == kNormal);
    state_ = kFrozen;
}

bool LogSpaceBase::Finalize(uint32_t final_metalog_position,
                            const std::vector<MetaLogProto>& tail_metalogs) {
    DCHECK(state_ == kNormal || state_ == kFrozen);
    for (const MetaLogProto& meta_log : tail_metalogs) {
        ProvideMetaLog(meta_log);
    }
    if (metalog_position_ == final_metalog_position) {
        state_ = kFinalized;
        OnFinalized();
        return true;
    } else {
        return false;
    }
}

void LogSpaceBase::SerializeToProto(MetaLogsProto* meta_logs_proto) {
    DCHECK(state_ == kFinalized && mode_ == kFullMode);
    meta_logs_proto->Clear();
    for (const MetaLogProto* metalog : applied_metalogs_) {
        meta_logs_proto->add_metalogs()->CopyFrom(*metalog);
    }
}

void LogSpaceBase::AdvanceMetaLogProgress() {
    auto iter = pending_metalogs_.begin();
    while (iter != pending_metalogs_.end()) {
        if (iter->first < metalog_position_) {
            iter = pending_metalogs_.erase(iter);
            continue;
        }
        MetaLogProto* meta_log = iter->second;
        if (!CanApplyMetaLog(*meta_log)) {
            break;
        }
        ApplyMetaLog(*meta_log);
        switch (mode_) {
        case kLiteMode:
            metalog_pool_.Return(meta_log);
            break;
        case kFullMode:
            DCHECK_EQ(size_t{metalog_position_}, applied_metalogs_.size());
            applied_metalogs_.push_back(meta_log);
            break;
        default:
            UNREACHABLE();
        }
        metalog_position_ = meta_log->metalog_seqnum() + 1;
        iter = pending_metalogs_.erase(iter);
    }
}

bool LogSpaceBase::CanApplyMetaLog(const MetaLogProto& meta_log) {
    switch (mode_) {
    case kLiteMode:
        switch (meta_log.type()) {
        case MetaLogProto::NEW_LOGS:
            for (size_t shard_idx : interested_shards_) {
                uint32_t shard_start = meta_log.new_logs_proto().shard_starts(shard_idx);
                DCHECK_GE(shard_start, shard_progrsses_[shard_idx]);
                if (shard_start > shard_progrsses_[shard_idx]) {
                    return false;
                }
            }
            return true;
        default:
            break;
        }
        break;
    case kFullMode:
        return meta_log.metalog_seqnum() == metalog_position_;
    default:
        break;
    }
    UNREACHABLE();
}

void LogSpaceBase::ApplyMetaLog(const MetaLogProto& meta_log) {
    switch (meta_log.type()) {
    case MetaLogProto::NEW_LOGS:
        {
            const auto& new_logs = meta_log.new_logs_proto();
            const View::NodeIdVec& engine_node_ids = view_->GetEngineNodes();
            uint32_t start_seqnum = new_logs.start_seqnum();
            for (size_t i = 0; i < engine_node_ids.size(); i++) {
                uint64_t start_localid = BuildLocalId(
                    view_->id(), engine_node_ids[i], new_logs.shard_starts(i));
                uint32_t delta = new_logs.shard_deltas(i);
                if (mode_ == kFullMode) {
                    OnNewLogs(start_seqnum, start_localid, delta);
                } else if (interested_shards_.contains(i)) {
                    shard_progrsses_[i] = new_logs.shard_starts(i) + delta;
                }
                start_seqnum += delta;
            }
            DCHECK_GT(start_seqnum, seqnum_position_);
            seqnum_position_ = start_seqnum;
        }
        break;
    case MetaLogProto::TRIM:
        DCHECK(mode_ == kFullMode);
        {
            const auto& trim = meta_log.trim_proto();
            OnTrim(trim.user_logspace(), trim.user_tag(), trim.trim_seqnum());
        }
        break;
    default:
        UNREACHABLE();
    }
}

MetaLogPrimary::MetaLogPrimary(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      sequencer_node_(view->GetSequencerNode(sequencer_id)) {
    log_header_ = fmt::format("MetaLogPrimary[{}]: ", view->id());
    state_ = kNormal;
}

MetaLogBackup::MetaLogBackup(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id) {
    log_header_ = fmt::format("MetaLogBackup[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

LogProducer::LogProducer(uint16_t engine_id, const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kLiteMode, view, sequencer_id) {
    AddInterestedShard(engine_id);
    log_header_ = fmt::format("LogProducer[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

LogStorage::LogStorage(uint16_t storage_id, const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kLiteMode, view, sequencer_id),
      storage_node_(view_->GetStorageNode(storage_id)),
      persisted_seqnum_position_(0) {
    for (uint16_t engine_id : storage_node_->GetSourceEngineNodes()) {
        AddInterestedShard(engine_id);
        shard_progrsses_[engine_id] = 0;
    }
    log_header_ = fmt::format("LogStorage[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

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
    shard_progrsses_[engine_id] = current;
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
