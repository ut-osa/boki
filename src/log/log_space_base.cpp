#include "log/log_space_base.h"

#include "utils/bits.h"

#define LOG_HEADER log_header_

namespace faas {
namespace log {

LogSpaceBase::LogSpaceBase(Mode mode, const View* view, uint16_t sequencer_id)
    : mode_(mode),
      state_(kCreated),
      view_(view),
      sequencer_node_(view->GetSequencerNode(sequencer_id)),
      metalog_position_(0),
      log_header_(fmt::format("LogSpace[{}-{}]: ", view->id(), sequencer_id)),
      shard_progrsses_(view->num_engine_nodes(), 0),
      seqnum_position_(0) {}

void LogSpaceBase::AddInterestedShard(uint16_t engine_id) {
    DCHECK(state_ == kCreated);
    const View::NodeIdVec& engine_node_ids = view_->GetEngineNodes();
    size_t idx = static_cast<size_t>(
        absl::c_find(engine_node_ids, engine_id) - engine_node_ids.begin());
    DCHECK_LT(idx, engine_node_ids.size());
    interested_shards_.insert(idx);
}

std::optional<MetaLogProto> LogSpaceBase::GetMetaLog(uint32_t pos) const {
    DCHECK(mode_ == kFullMode);
    if (pos >= metalog_position_) {
        return std::nullopt;
    }
    return *applied_metalogs_.at(pos);
}

bool LogSpaceBase::GetMetaLogs(uint32_t start_pos, uint32_t end_pos,
                               MetaLogProtoVec* metalogs) const {
    DCHECK(mode_ == kFullMode);
    DCHECK_LT(start_pos, end_pos);
    if (end_pos > metalog_position_) {
        return false;
    }
    DCHECK_NOTNULL(metalogs)->Clear();
    int vec_size = static_cast<int>(end_pos - start_pos);
    metalogs->Reserve(vec_size);
    for (uint32_t pos = start_pos; pos < end_pos; pos++) {
        MetaLogProto* metalog = applied_metalogs_.at(pos);
        metalogs->Add()->CopyFrom(*metalog);
    }
    DCHECK_EQ(metalogs->size(), vec_size);
    return true;
}

void LogSpaceBase::GetMetaLogsChecked(uint32_t start_pos, uint32_t end_pos,
                                      MetaLogProtoVec* metalogs) const {
    CHECK(GetMetaLogs(start_pos, end_pos, metalogs));
}

bool LogSpaceBase::ProvideMetaLog(const MetaLogProto& meta_log) {
    DCHECK(state_ == kNormal || state_ == kFrozen);
    if (mode_ == kLiteMode && meta_log.type() == MetaLogProto::TRIM) {
        HLOG_F(WARNING, "Trim log (seqnum={}) is simply ignore in lite mode",
               meta_log.metalog_seqnum());
        return false;
    }
    uint32_t seqnum = meta_log.metalog_seqnum();
    if (seqnum < metalog_position_) {
        return false;
    }
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
    HLOG_F(INFO, "Finalize log space, final_position={}, provided_tails={}",
           final_metalog_position, tail_metalogs.size());
    if (metalog_position_ == final_metalog_position) {
        OnFinalized(metalog_position_);
        return true;
    }
    if (metalog_position_ > final_metalog_position) {
        if (metalog_position_ > final_metalog_position + 1) {
            HLOG_F(FATAL, "See the future: current_position={}, expected_position={}",
                   metalog_position_, final_metalog_position);
        }
        // TODO: try fix this
        HLOG(WARNING) << "Fine, the problem with primary sequencer";
        OnFinalized(metalog_position_);
        return true;
    }
    for (const MetaLogProto& meta_log : tail_metalogs) {
        ProvideMetaLog(meta_log);
    }
    if (metalog_position_ < final_metalog_position) {
        HLOG_F(ERROR, "Metalog entries not sufficient: current_position={}, expected_position={}",
               metalog_position_, final_metalog_position);
        return false;
    } else if (metalog_position_ > final_metalog_position) {
        HLOG_F(FATAL, "It's uncommon, current_position={}, expected_position={}",
               metalog_position_, final_metalog_position);
    }
    OnFinalized(metalog_position_);
    return true;
}

void LogSpaceBase::SerializeToProto(MetaLogsProto* meta_logs_proto) {
    DCHECK(state_ == kFinalized && mode_ == kFullMode);
    meta_logs_proto->Clear();
    meta_logs_proto->set_logspace_id(identifier());
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
        OnMetaLogApplied(*meta_log);
        iter = pending_metalogs_.erase(iter);
    }
}

bool LogSpaceBase::CanApplyMetaLog(const MetaLogProto& meta_log) {
    switch (mode_) {
    case kLiteMode:
        switch (meta_log.type()) {
        case MetaLogProto::NEW_LOGS:
            for (size_t shard_idx : interested_shards_) {
                uint32_t shard_start = meta_log.new_logs_proto().shard_starts(
                    static_cast<int>(shard_idx));
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
            HVLOG_F(1, "Apply NEW_LOGS meta log: metalog_seqnum={}, start_seqnum={}",
                    meta_log.metalog_seqnum(), start_seqnum);
            for (size_t i = 0; i < engine_node_ids.size(); i++) {
                uint32_t shard_start = new_logs.shard_starts(static_cast<int>(i));
                uint32_t delta = new_logs.shard_deltas(static_cast<int>(i));
                uint64_t start_localid = bits::JoinTwo32(engine_node_ids[i], shard_start);
                if (mode_ == kFullMode || interested_shards_.contains(i)) {
                    OnNewLogs(meta_log.metalog_seqnum(),
                              bits::JoinTwo32(identifier(), start_seqnum),
                              start_localid, delta);
                }
                shard_progrsses_[i] = shard_start + delta;
                start_seqnum += delta;
            }
            DCHECK_GT(start_seqnum, seqnum_position_);
            seqnum_position_ = start_seqnum;
        }
        break;
    case MetaLogProto::TRIM:
        DCHECK(mode_ == kFullMode);
        OnTrim(meta_log.metalog_seqnum(), meta_log.trim_proto());
        break;
    default:
        UNREACHABLE();
    }
}

}  // namespace log
}  // namespace faas
