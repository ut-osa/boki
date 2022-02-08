#include "log/log_space.h"

#include "log/flags.h"

#define LOG_HEADER log_header_

namespace faas {
namespace log {

MetaLogPrimary::MetaLogPrimary(const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      persisted_metalog_position_(0),
      replicated_metalog_position_(0),
      cut_delta_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("log_space[{}-{}]: cut_delta", view->id(), sequencer_id))) {
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
    if (metalog_progresses_.size() < 2) {
        LOG(FATAL) << "Metalog needs at least 3 replicas!";
    }
    state_ = kNormal;
}

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

void MetaLogPrimary::UpdateReplicaProgress(uint16_t sequencer_id, uint32_t metalog_position) {
    if (!sequencer_node_->IsReplicaSequencerNode(sequencer_id)) {
        HLOG_F(FATAL, "Should not receive META_PROG message from sequencer {}", sequencer_id);
    }
    if (metalog_position > metalog_position_) {
        HLOG_F(FATAL, "Receive future position: received={}, current={}",
               metalog_position, metalog_position_);
    }
    DCHECK(metalog_progresses_.contains(sequencer_id));
    if (metalog_position > metalog_progresses_[sequencer_id]) {
        metalog_progresses_[sequencer_id] = metalog_position;
        UpdateMetaLogReplicatedPosition();
    }
}

void MetaLogPrimary::MarkMetaLogPersisted(uint32_t metalog_seqnum) {
    if (metalog_seqnum >= metalog_position()) {
        HLOG_F(FATAL, "Receive future position: seqnum={}, current_position={}",
               metalog_seqnum, metalog_position());
    }
    persisted_metalog_seqnums_.insert(metalog_seqnum);
    bool updated = false;
    auto iter = persisted_metalog_seqnums_.begin();
    while (iter != persisted_metalog_seqnums_.end()) {
        if (*iter == persisted_metalog_position_) {
            persisted_metalog_position_++;
            updated = true;
            iter = persisted_metalog_seqnums_.erase(iter);
        } else {
            break;
        }
    }
    if (updated) {
        UpdateMetaLogReplicatedPosition();
    }
}

void MetaLogPrimary::GrabNewlyReplicatedMetalogs(MetaLogProtoVec* newly_replicated_metalogs) {
    DCHECK(newly_replicated_metalogs != nullptr && newly_replicated_metalogs->empty());
    if (!replicated_metalogs_.empty()) {
        newly_replicated_metalogs->Swap(&replicated_metalogs_);
    }
}

std::optional<MetaLogProto> MetaLogPrimary::MarkNextCut() {
    if (dirty_shards_.empty()) {
        return std::nullopt;
    }
    if (previous_cut_seqnum_.has_value()
            && previous_cut_seqnum_.value() >= replicated_metalog_position_) {
        HLOG(INFO) << "The previous cut meta log not replicated, will not mark new cut";
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
    cut_delta_stat_.AddSample(total_delta);
    previous_cut_seqnum_ = meta_log_proto.metalog_seqnum();
    return meta_log_proto;
}

MetaLogProto MetaLogPrimary::AppendTrimOp(const MetaLogProto::TrimProto& trim_op) {
    MetaLogProto meta_log_proto;
    meta_log_proto.set_logspace_id(identifier());
    meta_log_proto.set_metalog_seqnum(metalog_position());
    meta_log_proto.set_type(MetaLogProto::TRIM);
    auto* trim_proto = meta_log_proto.mutable_trim_proto();
    trim_proto->CopyFrom(trim_op);
    HVLOG_F(1, "Generate new TRIM meta log: engine_id={}, op_id={}, trim_seqnum={}",
            trim_proto->engine_id(), trim_proto->trim_op_id(),
            bits::HexStr0x(trim_proto->trim_seqnum()));
    if (!ProvideMetaLog(meta_log_proto)) {
        HLOG(FATAL) << "Failed to advance metalog position";
    }
    return meta_log_proto;
}

void MetaLogPrimary::UpdateMetaLogReplicatedPosition() {
    if (replicated_metalog_position_ == persisted_metalog_position_) {
        return;
    }
    uint32_t old_position = replicated_metalog_position_;
    absl::InlinedVector<uint32_t, 8> tmp;
    tmp.reserve(metalog_progresses_.size() + 1);
    for (const auto& [sequencer_id, progress] : metalog_progresses_) {
        tmp.push_back(progress);
    }
    tmp.push_back(persisted_metalog_position_);
    absl::c_sort(tmp);
    uint32_t progress = tmp.at(tmp.size() / 2);
    progress = std::min(progress, persisted_metalog_position_);
    DCHECK_GE(progress, replicated_metalog_position_);
    DCHECK_LE(progress, metalog_position_);
    replicated_metalog_position_ = progress;
    DCHECK_LE(replicated_metalog_position_, persisted_metalog_position_);
    uint32_t new_position = replicated_metalog_position_;
    DCHECK_LE(old_position, new_position);
    for (uint32_t pos = old_position; pos < new_position; pos++) {
        auto meta_log = GetMetaLog(pos);
        DCHECK(meta_log.has_value());
        replicated_metalogs_.Add()->CopyFrom(*meta_log);
    }
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

LogProducer::LogProducer(uint16_t engine_id, const View* view, uint16_t sequencer_id)
    : LogSpaceBase(LogSpaceBase::kLiteMode, view, sequencer_id),
      next_localid_(bits::JoinTwo32(engine_id, 0)) {
    AddInterestedShard(engine_id);
    log_header_ = fmt::format("LogProducer[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

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
    : LogSpaceBase(LogSpaceBase::kFullMode, view, sequencer_id),
      storage_node_(view_->GetStorageNode(storage_id)),
      shard_progrss_dirty_(false),
      persisted_seqnum_position_(bits::JoinTwo32(identifier(), 0)),
      max_live_entries_(absl::GetFlag(FLAGS_slog_storage_max_live_entries)),
      target_live_entries_(absl::GetFlag(FLAGS_slog_storage_target_live_entries)),
      live_entries_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          fmt::format("log_space[{}-{}]: live_entries", view->id(), sequencer_id))) {
    for (uint16_t engine_id : storage_node_->GetSourceEngineNodes()) {
        shard_progrsses_[engine_id] = 0;
    }
    index_data_.set_logspace_id(identifier());
    log_header_ = fmt::format("LogStorage[{}-{}]: ", view->id(), sequencer_id);
    state_ = kNormal;
}

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
    ReadResult result;
    result.status = ReadResult::kFailed;
    result.original_request = request;
    if (live_log_entries_.contains(seqnum)) {
        const Entry* entry = live_log_entries_.at(seqnum);
        result.status = ReadResult::kInlined;
        result.localid = entry->metadata.localid;
        result.data = entry->data;
    } else if (seqnum < persisted_seqnum_position_) {
        result.status = ReadResult::kLookupDB;
    } else {
        HLOG_F(WARNING, "Failed to locate seqnum {}", bits::HexStr0x(seqnum));
    }
    pending_read_results_.push_back(std::move(result));
}

void LogStorage::GrabLogEntriesForPersistence(LogEntryVec* log_entires) {
    DCHECK(log_entires != nullptr && log_entires->empty());
    if (entries_for_persistence_.empty()) {
        return;
    }
    *log_entires = std::move(entries_for_persistence_);
    entries_for_persistence_.clear();
}

void LogStorage::LogEntriesPersisted(std::span<const uint64_t> seqnums) {
    if (seqnums.empty()) {
        return;
    }
    uint64_t previous_head = kInvalidLogSeqNum;
    if (!persisted_seqnums_.empty()) {
        previous_head = *(persisted_seqnums_.begin());
    }
    for (const uint64_t seqnum : seqnums) {
        DCHECK(persisted_seqnums_.count(seqnum) == 0);
        persisted_seqnums_.insert(seqnum);
    }
    if (*(persisted_seqnums_.begin()) == previous_head) {
        return;
    }
    auto live_iter = absl::c_lower_bound(live_seqnums_, persisted_seqnum_position_);
    auto persisted_iter = persisted_seqnums_.begin();
    while (persisted_iter != persisted_seqnums_.end()) {
        DCHECK(live_iter != live_seqnums_.end());
        if (*live_iter == *persisted_iter) {
            persisted_seqnum_position_ = *live_iter + 1;
            live_iter++;
            persisted_iter = persisted_seqnums_.erase(persisted_iter);
        } else {
            DCHECK_GT(*persisted_iter, *live_iter);
            break;
        }
    }
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

LogStorage::TrimOpVec LogStorage::FetchTrimOps() const {
    TrimOpVec trim_ops;
    for (const auto& [user_logspace, trim_seqnum] : trim_seqnums_) {
        trim_ops.push_back({
            .user_logspace = user_logspace,
            .trim_seqnum = trim_seqnum,
        });
    }
    return trim_ops;
}

void LogStorage::MarkTrimOpFinished(const TrimOp& trim_op) {
    DCHECK(trim_seqnums_.contains(trim_op.user_logspace));
    DCHECK_GE(trim_seqnums_.at(trim_op.user_logspace), trim_op.trim_seqnum);
    if (trim_op.trim_seqnum == trim_seqnums_.at(trim_op.user_logspace)) {
        trim_seqnums_.erase(trim_op.user_logspace);
    }
}

std::optional<std::vector<uint32_t>> LogStorage::GrabShardProgressForSending() {
    if (!shard_progrss_dirty_) {
        return std::nullopt;
    }
    if (live_seqnums_.size() > max_live_entries_) {
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
        ReadResult result;
        result.status = ReadResult::kFailed;
        result.original_request = iter->second;
        pending_read_results_.push_back(std::move(result));
        iter = pending_read_requests_.erase(iter);
    }
    uint16_t engine_id = gsl::narrow_cast<uint16_t>(bits::HighHalf64(start_localid));
    if (!storage_node_->IsSourceEngineNode(engine_id)) {
        return;
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
            pending_read_results_.push_back(ReadResult {
                .status = ReadResult::kInlined,
                .localid = localid,
                .data = log_entry->data,
                .original_request = iter->second
            });
            iter = pending_read_requests_.erase(iter);
        }
    }
}

void LogStorage::OnTrim(uint32_t metalog_seqnum, const MetaLogProto::TrimProto& trim_proto) {
    HVLOG_F(1, "Receive trim metalog entry: metalog_seqnum={}, "
               "user_logspace={}, trim_seqnum={}, tag={}",
            bits::HexStr0x(metalog_seqnum),
            trim_proto.user_logspace(),
            bits::HexStr0x(trim_proto.trim_seqnum()),
            trim_proto.user_tag());
    if (trim_proto.user_tag() != kEmptyLogTag) {
        // Ignore Trim with non-zero tag for now
        return;
    }
    uint32_t user_logspace = trim_proto.user_logspace();
    uint64_t trim_seqnum = std::min(seqnum_position(), trim_proto.trim_seqnum());
    if (!trim_seqnums_.contains(user_logspace)
          || trim_seqnum > trim_seqnums_.at(user_logspace)) {
        trim_seqnums_[user_logspace] = trim_seqnum;
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
    while (live_seqnums_.size() > target_live_entries_) {
        uint64_t seqnum = live_seqnums_.front();
        if (seqnum >= persisted_seqnum_position_) {
            break;
        }
        Entry* log_entry = const_cast<Entry*>(live_log_entries_.at(seqnum));
        live_log_entries_.erase(seqnum);
        live_seqnums_.pop_front();
        DCHECK_EQ(live_seqnums_.size(), live_log_entries_.size());
        ClearLogData(&log_entry->data);
        entry_pool_.Return(log_entry);
    }
    live_entries_stat_.AddSample(gsl::narrow_cast<int>(live_seqnums_.size()));
}

void LogStorage::ClearLogData(LogData* data) {
    if (auto str = std::get_if<std::string>(data); str != nullptr) {
        str->clear();
    }
    if (auto record = std::get_if<JournalRecord>(data); record != nullptr) {
        record->file.Clear();
    }
}

}  // namespace log
}  // namespace faas
