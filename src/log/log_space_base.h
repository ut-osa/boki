#pragma once

#include "log/common.h"
#include "log/view.h"
#include "utils/object_pool.h"
#include "utils/bits.h"

namespace faas {
namespace log {

class LogSpaceBase {
public:
    virtual ~LogSpaceBase();

    uint16_t view_id() const { return view_->id(); }
    uint16_t sequencer_id() const { return sequencer_id_; }
    uint32_t identifier() const {
        return bits::JoinTwo16(view_->id(), sequencer_id_);
    }

    uint32_t metalog_position() const { return metalog_position_; }
    uint32_t seqnum_position() const { return seqnum_position_; }
    // Return true if metalog_position changed
    bool ProvideMetaLog(const MetaLogProto& meta_log_proto);

    bool frozen() const { return state_ == kFrozen; }
    bool finalized() const { return state_ == kFinalized; }

    void Freeze();
    bool Finalize(uint32_t final_metalog_position,
                  const std::vector<MetaLogProto>& tail_metalogs);

    void SerializeToProto(MetaLogsProto* meta_logs_proto);

protected:
    enum Mode { kLiteMode, kFullMode };
    enum State { kCreated, kNormal, kFrozen, kFinalized };

    LogSpaceBase(Mode mode, const View* view, uint16_t sequencer_id);
    void AddInterestedShard(uint16_t engine_id);

    typedef absl::FixedArray<uint32_t> OffsetVec;
    virtual void OnNewLogs(uint32_t start_seqnum, uint64_t start_localid,
                           uint32_t delta) {}
    virtual void OnTrim(uint32_t user_logspace, uint64_t user_tag,
                        uint64_t trim_seqnum) {}
    virtual void OnFinalized() {}

    virtual bool CanApplyMetaLog(const MetaLogProto& meta_log);
    void AdvanceMetaLogProgress();

    Mode mode_;
    State state_;
    const View* view_;
    uint16_t sequencer_id_;
    uint32_t metalog_position_;
    uint32_t seqnum_position_;
    std::string log_header_;

private:
    absl::flat_hash_set<size_t> interested_shards_;
    absl::FixedArray<uint32_t> shard_progrsses_;

    utils::ProtobufMessagePool<MetaLogProto> metalog_pool_;
    std::vector<MetaLogProto*> applied_metalogs_;
    std::map</* metalog_seqnum */ uint32_t, MetaLogProto*> pending_metalogs_;

    void ApplyMetaLog(const MetaLogProto& meta_log);

    DISALLOW_COPY_AND_ASSIGN(LogSpaceBase);
};

}  // namespace log
}  // namespace faas
