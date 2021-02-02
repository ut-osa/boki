#include "log/log_space.h"

#include "log/flags.h"

namespace faas {
namespace log {

MetaLogPrimary::MetaLogPrimary(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      replicated_metalog_position_(0) {
    for (uint16_t engine_id : view_->GetEngineNodes()) {
        const View::Engine* engine_node = view_->GetEngineNode(engine_id);
        for (uint16_t storage_id : engine_node->GetStorageNodes()) {
            auto pair = std::make_pair(engine_id, storage_id);
            shard_progrsses_[pair] = 0;
        }
        last_cut_[engine_id] = 0;
    }
    for (uint16_t sequencer_id : sequencer_node_->GetReplicaSequencerNodes()) {
        metalog_progresses_[sequencer_id] = 0;
    }
    log_header_ = fmt::format("MetaLogPrimary[{}]: ", view->id());
    if (metalog_progresses_.empty()) {
        HLOG(WARNING) << "No meta log replication";
    }
    state_ = kNormal;
}

MetaLogPrimary::~MetaLogPrimary() {}

void MetaLogPrimary::UpdateStorageProgress(uint16_t storage_id,
                                           const std::vector<uint32_t>& progress) {
    if (!view_->contains_storage_node(storage_id)) {
        HLOG(FATAL) << fmt::format("View {} does not has storage node {}",
                                   view_->id(), storage_id);
    }
    const View::Storage* storage_node = view_->GetStorageNode(storage_id);
    const View::NodeIdVec& engine_node_ids = storage_node->GetSourceEngineNodes();
    if (progress.size() != engine_node_ids.size()) {
        HLOG(FATAL) << fmt::format("Size does not match: have={}, expected={}",
                                   progress.size(), engine_node_ids.size());
    }
    for (size_t i = 0; i < progress.size(); i++) {
        uint16_t engine_id = engine_node_ids[i];
        auto pair = std::make_pair(engine_id, storage_id);
        DCHECK(shard_progrsses_.contains(pair));
        if (progress[i] > shard_progrsses_[pair]) {
            shard_progrsses_[pair] = progress[i];
            uint32_t current_position = GetShardReplicatedPosition(engine_id);
            DCHECK_GE(current_position, last_cut_.at(engine_id));
            if (current_position > last_cut_.at(engine_id)) {
                HVLOG(1) << fmt::format("Store progress from storage {} for engine {}: {}",
                                        storage_id, engine_id,
                                        bits::HexStr0x(current_position));
                dirty_shards_.insert(engine_id);
            }
        }
    }
}

void MetaLogPrimary::UpdateReplicaProgress(uint16_t sequencer_id,
                                           uint32_t metalog_position) {
    if (!sequencer_node_->IsReplicaSequencerNode(sequencer_id)) {
        HLOG(FATAL) << fmt::format("Should not receive META_PROG message from sequencer {}",
                                    sequencer_id);
    }
    if (metalog_position > metalog_position_) {
        HLOG(FATAL) << fmt::format("Receive future position: received={}, current={}",
                                   metalog_position, metalog_position_);
    }
    if (metalog_position > metalog_progresses_[sequencer_id]) {
        metalog_progresses_[sequencer_id] = metalog_position;
        UpdateMetaLogReplicatedPosition();
    }
}

bool MetaLogPrimary::MarkNextCut(MetaLogProto* meta_log_proto) {
    if (dirty_shards_.empty()) {
        return false;
    }
    meta_log_proto->Clear();
    meta_log_proto->set_logspace_id(identifier());
    meta_log_proto->set_metalog_seqnum(metalog_position());
    meta_log_proto->set_type(MetaLogProto::NEW_LOGS);
    auto* new_logs_proto = meta_log_proto->mutable_new_logs_proto();
    new_logs_proto->set_start_seqnum(bits::LowHalf64(seqnum_position()));
    uint32_t total_delta = 0;
    for (uint16_t engine_id : view_->GetEngineNodes()) {
        new_logs_proto->add_shard_starts(last_cut_.at(engine_id));
        uint32_t delta = 0;
        if (dirty_shards_.contains(engine_id)) {
            uint32_t current_position = GetShardReplicatedPosition(engine_id);
            DCHECK_GT(current_position, last_cut_.at(engine_id));
            delta = current_position - last_cut_.at(engine_id);
            last_cut_[engine_id] = current_position;
        }
        new_logs_proto->add_shard_deltas(delta);
        total_delta += delta;
    }
    dirty_shards_.clear();
    HVLOG(1) << fmt::format("Generate new NEW_LOGS meta log: "
                            "start_seqnum={}, total_delta={}",
                            new_logs_proto->start_seqnum(), total_delta);
    if (!ProvideMetaLog(*meta_log_proto)) {
        HLOG(FATAL) << "Failed to advance metalog position";
    }
    DCHECK_EQ(new_logs_proto->start_seqnum() + total_delta,
              bits::LowHalf64(seqnum_position()));
    return true;
}

void MetaLogPrimary::UpdateMetaLogReplicatedPosition() {
    if (replicated_metalog_position_ == metalog_position_) {
        return;
    }
    if (metalog_progresses_.empty()) {
        return;
    }
    std::vector<uint32_t> tmp;
    tmp.reserve(metalog_progresses_.size());
    for (const auto& [sequencer_id, progress] : metalog_progresses_) {
        tmp.push_back(progress);
    }
    absl::c_sort(tmp);
    uint32_t progress = tmp.at(tmp.size() / 2);
    DCHECK_GE(progress, replicated_metalog_position_);
    DCHECK_LE(progress, metalog_position_);
    replicated_metalog_position_ = progress;
}

uint32_t MetaLogPrimary::GetShardReplicatedPosition(uint16_t engine_id) const {
    uint32_t min_value = std::numeric_limits<uint32_t>::max();
    const View::Engine* engine_node = view_->GetEngineNode(engine_id);
    for (uint16_t storage_id : engine_node->GetStorageNodes()) {
        auto pair = std::make_pair(engine_id, storage_id);
        DCHECK(shard_progrsses_.contains(pair));
        min_value = std::min(min_value, shard_progrsses_.at(pair));
    }
    DCHECK_LT(min_value, std::numeric_limits<uint32_t>::max());
    return min_value;
}

MetaLogBackup::MetaLogBackup(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id) {
    log_header_ = fmt::format("MetaLogBackup[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

MetaLogBackup::~MetaLogBackup() {}

LogProducer::LogProducer(uint16_t engine_id, const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kLiteMode, view, sequencer_id),
      next_localid_(bits::JoinTwo32(engine_id, 0)) {
    AddInterestedShard(engine_id);
    log_header_ = fmt::format("LogProducer[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

LogProducer::~LogProducer() {}

void LogProducer::LocalAppend(void* caller_data, uint64_t* localid) {
    DCHECK(!pending_appends_.contains(next_localid_));
    HVLOG(1) << fmt::format("LocalAppend with localid {}", bits::HexStr0x(next_localid_));
    pending_appends_[next_localid_] = caller_data;
    *localid = next_localid_++;
}

void LogProducer::PollAppendResults(AppendResultVec* results) {
    *results = std::move(pending_append_results_);
    pending_append_results_.clear();
}

void LogProducer::OnNewLogs(uint32_t metalog_seqnum,
                            uint64_t start_seqnum, uint64_t start_localid,
                            uint32_t delta) {
    for (size_t i = 0; i < delta; i++) {
        uint64_t seqnum = start_seqnum + i;
        uint64_t localid = start_localid + i;
        if (!pending_appends_.contains(localid)) {
            HLOG(FATAL) << fmt::format("Cannot find pending log entry for localid {}",
                                       bits::HexStr0x(localid));
        }
        pending_append_results_.push_back(AppendResult {
            .seqnum = seqnum,
            .localid = localid,
            .metalog_progress = bits::JoinTwo32(identifier(), metalog_seqnum + 1),
            .caller_data = pending_appends_[localid]
        });
        pending_appends_.erase(localid);
    }
}

void LogProducer::OnFinalized(uint32_t metalog_position) {
    for (const auto& [localid, caller_data] : pending_appends_) {
        pending_append_results_.push_back(AppendResult {
            .seqnum = kInvalidLogSeqNum,
            .localid = localid,
            .metalog_progress = 0,
            .caller_data = caller_data
        });
    }
    pending_appends_.clear();
}

LogStorage::LogStorage(uint16_t storage_id, const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kLiteMode, view, sequencer_id),
      storage_node_(view_->GetStorageNode(storage_id)),
      shard_progrss_dirty_(false),
      persisted_seqnum_position_(0) {
    for (uint16_t engine_id : storage_node_->GetSourceEngineNodes()) {
        AddInterestedShard(engine_id);
        shard_progrsses_[engine_id] = 0;
    }
    index_data_.set_logspace_id(identifier());
    log_header_ = fmt::format("LogStorage[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

LogStorage::~LogStorage() {}

bool LogStorage::Store(const LogMetaData& log_metadata,
                       std::span<const char> log_data) {
    uint64_t localid = log_metadata.localid;
    DCHECK_EQ(size_t{log_metadata.data_size}, log_data.size());
    uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
    HVLOG(1) << fmt::format("Store log from engine {} with localid {}",
                            engine_id, bits::HexStr0x(localid));
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
    DCHECK_EQ(request.logspace_id, identifier());
    uint64_t seqnum = bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf);
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
    } else {
        HLOG(WARNING) << fmt::format("Failed to locate seqnum {}", bits::HexStr0x(seqnum));
    }
    pending_read_results_.push_back(std::move(result));
}

bool LogStorage::GrabLogEntriesForPersistence(
        std::vector<std::shared_ptr<const LogEntry>>* log_entries,
        uint64_t* new_position) const {
    if (live_seqnums_.empty() || live_seqnums_.back() < persisted_seqnum_position_) {
        return false;
    }
    auto iter = absl::c_lower_bound(live_seqnums_, persisted_seqnum_position_);
    DCHECK(iter != live_seqnums_.end());
    DCHECK_GE(*iter, persisted_seqnum_position_);
    log_entries->clear();
    while (iter != live_seqnums_.end()) {
        uint64_t seqnum = *(iter++);
        DCHECK(live_log_entries_.contains(seqnum));
        log_entries->push_back(live_log_entries_.at(seqnum));
    }
    DCHECK(!log_entries->empty());
    *new_position = live_seqnums_.back() + 1;
    return true;
}

void LogStorage::LogEntriesPersisted(uint64_t new_position) {
    persisted_seqnum_position_ = new_position;
    ShrinkLiveEntriesIfNeeded();
}

void LogStorage::PollReadResults(ReadResultVec* results) {
    *results = std::move(pending_read_results_);
    pending_read_results_.clear();
}

bool LogStorage::PollIndexData(IndexDataProto* data) {
    if (index_data_.seqnum_halves_size() == 0) {
        return false;
    }
    data->Swap(&index_data_);
    index_data_.Clear();
    index_data_.set_logspace_id(identifier());
    return true;
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

void LogStorage::OnNewLogs(uint32_t metalog_seqnum,
                           uint64_t start_seqnum, uint64_t start_localid,
                           uint32_t delta) {
    auto iter = pending_read_requests_.begin();
    while (iter != pending_read_requests_.end() && iter->first < start_seqnum) {
        HLOG(WARNING) << fmt::format("Read request for seqnum {} has past",
                                     bits::HexStr0x(iter->first));
        pending_read_results_.push_back(ReadResult {
            .status = ReadResult::kFailed,
            .log_entry = nullptr,
            .original_request = iter->second
        });
        iter = pending_read_requests_.erase(iter);
    }
    for (size_t i = 0; i < delta; i++) {
        uint64_t seqnum = start_seqnum + i;
        uint64_t localid = start_localid + i;
        if (!pending_log_entries_.contains(localid)) {
            HLOG(FATAL) << fmt::format("Cannot find pending log entry for localid {}",
                                       bits::HexStr0x(localid));
        }
        // Build the log entry for live_log_entries_
        LogEntry* log_entry = pending_log_entries_[localid].release();
        pending_log_entries_.erase(localid);
        HVLOG(1) << fmt::format("Store the log entry (seqnum {})", bits::HexStr0x(seqnum));
        log_entry->metadata.seqnum = seqnum;
        std::shared_ptr<const LogEntry> log_entry_ptr(log_entry);
        // Add the new entry to index data
        index_data_.add_seqnum_halves(bits::LowHalf64(seqnum));
        index_data_.add_engine_ids(bits::HighHalf64(localid));
        index_data_.add_user_logspaces(log_entry->metadata.user_logspace);
        index_data_.add_user_tags(log_entry->metadata.user_tag);
        // Update live_seqnums_ and live_log_entries_
        DCHECK(live_seqnums_.empty() || seqnum > live_seqnums_.back());
        live_seqnums_.push_back(seqnum);
        live_log_entries_[seqnum] = log_entry_ptr;
        DCHECK_EQ(live_seqnums_.size(), live_log_entries_.size());
        ShrinkLiveEntriesIfNeeded();
        // Check if we have read request on it
        if (iter != pending_read_requests_.end() && iter->first == seqnum) {
            pending_read_results_.push_back(ReadResult {
                .status = ReadResult::kOK,
                .log_entry = log_entry_ptr,
                .original_request = iter->second
            });
            iter = pending_read_requests_.erase(iter);
        }
    }
}

void LogStorage::OnFinalized(uint32_t metalog_position) {
    if (!pending_log_entries_.empty()) {
        HLOG(WARNING) << fmt::format("{} pending log entries discarded",
                                     pending_log_entries_.size());
        pending_log_entries_.clear();
    }
    if (!pending_read_requests_.empty()) {
        HLOG(FATAL) << fmt::format("There are {} pending reads",
                                   pending_read_requests_.size());
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
