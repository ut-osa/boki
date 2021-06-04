#include "log/log_space.h"

#include "log/flags.h"

namespace faas {
namespace log {

MetaLogPrimary::MetaLogPrimary(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      replicated_metalog_position_(0) {
    log_header_ = fmt::format("MetaLogPrimary[{}]: ", view->id());
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
    metalog_progresses_[sequencer_node_->node_id()] = 0;
    if (metalog_progresses_.size() < 3) {
        LOG(FATAL) << "Metalog needs at least 3 replicas!";
    }
    state_ = kNormal;
}

MetaLogPrimary::~MetaLogPrimary() {}

void MetaLogPrimary::UpdateStorageProgress(uint16_t storage_id,
                                           const std::vector<uint32_t>& progress) {
    if (!view_->contains_storage_node(storage_id)) {
        HLOG_F(FATAL, "View {} does not has storage node {}", view_->id(), storage_id);
    }
    const View::Storage* storage_node = view_->GetStorageNode(storage_id);
    const View::NodeIdVec& engine_node_ids = storage_node->GetSourceEngineNodes();
    if (progress.size() != engine_node_ids.size()) {
        HLOG_F(FATAL, "Size does not match: have={}, expected={}",
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
                HVLOG_F(1, "Store progress from storage {} for engine {}: {}",
                        storage_id, engine_id, bits::HexStr0x(current_position));
                dirty_shards_.insert(engine_id);
            }
        }
    }
}

bool MetaLogPrimary::all_metalog_replicated() const {
    return metalog_progresses_.at(sequencer_id()) == metalog_position()
            && replicated_metalog_position_ == metalog_position();
}

void MetaLogPrimary::UpdateReplicaProgress(uint16_t sequencer_id, uint32_t metalog_position,
                                           MetaLogProtoVec* newly_replicated_metalogs) {
    if (sequencer_id != sequencer_node_->node_id() &&
          !sequencer_node_->IsReplicaSequencerNode(sequencer_id)) {
        HLOG_F(FATAL, "Should not receive META_PROG message from sequencer {}", sequencer_id);
    }
    if (metalog_position > metalog_position_) {
        HLOG_F(FATAL, "Receive future position: received={}, current={}",
               metalog_position, metalog_position_);
    }
    DCHECK(newly_replicated_metalogs == nullptr || newly_replicated_metalogs->empty());
    uint32_t old_position = replicated_metalog_position();
    DCHECK(metalog_progresses_.contains(sequencer_id));
    if (metalog_position > metalog_progresses_[sequencer_id]) {
        metalog_progresses_[sequencer_id] = metalog_position;
        UpdateMetaLogReplicatedPosition();
        uint32_t new_position = replicated_metalog_position();
        if (new_position > old_position) {
            GetMetaLogsChecked(old_position, new_position,
                               DCHECK_NOTNULL(newly_replicated_metalogs));
        }
    } else {
        DCHECK(sequencer_id != sequencer_node_->node_id())
            << fmt::format("current_progress={}, provided={}",
                           metalog_progresses_[sequencer_id],
                           metalog_position);
    }
}

std::optional<MetaLogProto> MetaLogPrimary::MarkNextCut() {
    if (dirty_shards_.empty()) {
        return std::nullopt;
    }
    MetaLogProto meta_log_proto;
    meta_log_proto.set_logspace_id(identifier());
    meta_log_proto.set_metalog_seqnum(metalog_position());
    meta_log_proto.set_type(MetaLogProto::NEW_LOGS);
    auto* new_logs_proto = meta_log_proto.mutable_new_logs_proto();
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
    HVLOG_F(1, "Generate new NEW_LOGS meta log: start_seqnum={}, total_delta={}",
            new_logs_proto->start_seqnum(), total_delta);
    if (!ProvideMetaLog(meta_log_proto)) {
        HLOG(FATAL) << "Failed to advance metalog position";
    }
    DCHECK_EQ(new_logs_proto->start_seqnum() + total_delta,
              bits::LowHalf64(seqnum_position()));
    return meta_log_proto;
}

void MetaLogPrimary::UpdateMetaLogReplicatedPosition() {
    if (replicated_metalog_position_ == metalog_position_) {
        return;
    }
    absl::InlinedVector<uint32_t, 8> tmp;
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
    HVLOG_F(1, "LocalAppend with localid {}", bits::HexStr0x(next_localid_));
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
            HLOG_F(FATAL, "Cannot find pending log entry for localid {}",
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
      persisted_seqnum_position_(0),
      live_entries_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          fmt::format("log_space[{}-{}]: live_entries", view->id(), sequencer_id))) {
    for (uint16_t engine_id : storage_node_->GetSourceEngineNodes()) {
        AddInterestedShard(engine_id);
        shard_progrsses_[engine_id] = 0;
    }
    index_data_.set_logspace_id(identifier());
    log_header_ = fmt::format("LogStorage[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

LogStorage::~LogStorage() {}

bool LogStorage::Store(Entry new_entry) {
    uint64_t localid = new_entry.metadata.localid;
    uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(localid));
    HVLOG_F(1, "Store log from engine {} with localid {}",
            engine_id, bits::HexStr0x(localid));
    if (!storage_node_->IsSourceEngineNode(engine_id)) {
        HLOG_F(ERROR, "Not storage node (node_id {}) for engine (node_id {})",
               storage_node_->node_id(), engine_id);
        return false;
    }
    Entry* entry = entry_pool_.Get();
    *entry = std::move(new_entry);
    entry->journal_file->Ref();
    pending_log_entries_[localid] = entry;
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
        .original_request = request
    };
    if (live_log_entries_.contains(seqnum)) {
        const Entry* entry = live_log_entries_.at(seqnum);
        entry->journal_file->Ref();
        result.status = ReadResult::kLookupJournal;
        result.localid = entry->metadata.localid;
        result.journal_file = entry->journal_file;
        result.journal_offset = entry->journal_offset;
    } else if (seqnum < persisted_seqnum_position_) {
        result.status = ReadResult::kLookupDB;
    } else {
        HLOG_F(WARNING, "Failed to locate seqnum {}", bits::HexStr0x(seqnum));
    }
    pending_read_results_.push_back(std::move(result));
}

void LogStorage::GrabLogEntriesForPersistence(LogEntryVec* log_entires) {
    if (entries_for_persistence_.empty()) {
        return;
    }
    *log_entires = std::move(entries_for_persistence_);
    entries_for_persistence_.clear();
}

void LogStorage::LogEntriesPersisted(uint64_t new_position) {
    persisted_seqnum_position_ = new_position;
    ShrinkLiveEntriesIfNeeded();
}

void LogStorage::PollReadResults(ReadResultVec* results) {
    *results = std::move(pending_read_results_);
    pending_read_results_.clear();
}

std::optional<IndexDataProto> LogStorage::PollIndexData() {
    if (index_data_.seqnum_halves_size() == 0) {
        return std::nullopt;
    }
    IndexDataProto data;
    data.Swap(&index_data_);
    index_data_.Clear();
    index_data_.set_logspace_id(identifier());
    return data;
}

std::optional<std::vector<uint32_t>> LogStorage::GrabShardProgressForSending() {
    if (!shard_progrss_dirty_) {
        return std::nullopt;
    }
    size_t max_size = absl::GetFlag(FLAGS_slog_storage_max_live_entries);
    if (live_seqnums_.size() > max_size * 2) {
        return std::nullopt;
    }
    std::vector<uint32_t> progress;
    progress.reserve(storage_node_->GetSourceEngineNodes().size());
    for (uint16_t engine_id : storage_node_->GetSourceEngineNodes()) {
        progress.push_back(shard_progrsses_[engine_id]);
    }
    shard_progrss_dirty_ = false;
    return progress;
}

void LogStorage::OnNewLogs(uint32_t metalog_seqnum,
                           uint64_t start_seqnum, uint64_t start_localid,
                           uint32_t delta) {
    auto iter = pending_read_requests_.begin();
    while (iter != pending_read_requests_.end() && iter->first < start_seqnum) {
        HLOG_F(WARNING, "Read request for seqnum {} has past", bits::HexStr0x(iter->first));
        pending_read_results_.push_back(ReadResult {
            .status = ReadResult::kFailed,
            .original_request = iter->second
        });
        iter = pending_read_requests_.erase(iter);
    }
    for (size_t i = 0; i < delta; i++) {
        uint64_t seqnum = start_seqnum + i;
        uint64_t localid = start_localid + i;
        if (!pending_log_entries_.contains(localid)) {
            HLOG_F(FATAL, "Cannot find pending log entry for localid {}",
                   bits::HexStr0x(localid));
        }
        // Build the log entry for live_log_entries_
        Entry* log_entry = pending_log_entries_[localid];
        pending_log_entries_.erase(localid);
        HVLOG_F(1, "Finalize the log entry (seqnum={}, localid={})",
                bits::HexStr0x(seqnum), bits::HexStr0x(localid));
        log_entry->metadata.seqnum = seqnum;
        // Add the new entry to index data
        index_data_.add_seqnum_halves(bits::LowHalf64(seqnum));
        index_data_.add_engine_ids(bits::HighHalf64(localid));
        index_data_.add_user_logspaces(log_entry->metadata.user_logspace);
        index_data_.add_user_tag_sizes(
            gsl::narrow_cast<uint32_t>(log_entry->user_tags.size()));
        index_data_.mutable_user_tags()->Add(
            log_entry->user_tags.begin(), log_entry->user_tags.end());
        // Update live_seqnums_ and live_log_entries_
        DCHECK(live_seqnums_.empty() || seqnum > live_seqnums_.back());
        live_seqnums_.push_back(seqnum);
        live_log_entries_[seqnum] = log_entry;
        DCHECK_EQ(live_seqnums_.size(), live_log_entries_.size());
        ShrinkLiveEntriesIfNeeded();
        // Will persist the new entry to DB
        DCHECK(entries_for_persistence_.empty()
                 || seqnum > entries_for_persistence_.back()->metadata.seqnum);
        entries_for_persistence_.push_back(log_entry);
        // Check if we have read request on it
        while (iter != pending_read_requests_.end() && iter->first == seqnum) {
            log_entry->journal_file->Ref();
            pending_read_results_.push_back(ReadResult {
                .status = ReadResult::kLookupJournal,
                .localid = localid,
                .journal_file = log_entry->journal_file,
                .journal_offset = log_entry->journal_offset,
                .original_request = iter->second
            });
            iter = pending_read_requests_.erase(iter);
        }
    }
}

void LogStorage::OnFinalized(uint32_t metalog_position) {
    if (!pending_log_entries_.empty()) {
        HLOG_F(WARNING, "{} pending log entries discarded", pending_log_entries_.size());
        pending_log_entries_.clear();
    }
    if (!pending_read_requests_.empty()) {
        HLOG_F(FATAL, "There are {} pending reads", pending_read_requests_.size());
    }
}

void LogStorage::AdvanceShardProgress(uint16_t engine_id) {
    uint32_t current = shard_progrsses_[engine_id];
    while (pending_log_entries_.contains(bits::JoinTwo32(engine_id, current))) {
        current++;
    }
    if (current > shard_progrsses_[engine_id]) {
        HVLOG_F(1, "Update shard progres for engine {}: from={}, to={}",
                engine_id,
                bits::HexStr0x(shard_progrsses_[engine_id]),
                bits::HexStr0x(current));
        shard_progrss_dirty_ = true;
        shard_progrsses_[engine_id] = current;
    }
}

void LogStorage::ShrinkLiveEntriesIfNeeded() {
    size_t max_size = absl::GetFlag(FLAGS_slog_storage_max_live_entries);
    while (live_seqnums_.size() > max_size) {
        uint64_t seqnum = live_seqnums_.front();
        if (seqnum >= persisted_seqnum_position_) {
            break;
        }
        const Entry* log_entry = live_log_entries_.at(seqnum);
        live_log_entries_.erase(seqnum);
        live_seqnums_.pop_front();
        DCHECK_EQ(live_seqnums_.size(), live_log_entries_.size());
        log_entry->journal_file->Unref();
        entry_pool_.Return(const_cast<Entry*>(log_entry));
    }
    live_entries_stat_.AddSample(gsl::narrow_cast<int>(live_seqnums_.size()));
}

}  // namespace log
}  // namespace faas
