#pragma once

#include "log/common.h"
#include "log/view.h"
#include "utils/lockable_ptr.h"
#include "utils/object_pool.h"
#include "utils/bits.h"

namespace faas {
namespace log {

class LogSpaceBase {
public:
    virtual ~LogSpaceBase();

    uint16_t view_id() const { return view_->id(); }
    uint16_t sequencer_id() const { return sequencer_node_->node_id(); }
    uint32_t identifier() const { return bits::JoinTwo16(view_id(), sequencer_id()); }

    uint32_t metalog_position() const { return metalog_position_; }
    uint64_t seqnum_position() const {
        return bits::JoinTwo32(identifier(), seqnum_position_);
    }

    bool GetMetaLogs(uint32_t start_pos, uint32_t end_pos,
                     std::vector<MetaLogProto>* metalogs) const;

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
    virtual void OnNewLogs(uint32_t metalog_seqnum,
                           uint64_t start_seqnum, uint64_t start_localid,
                           uint32_t delta) {}
    virtual void OnTrim(uint32_t metalog_seqnum,
                        uint32_t user_logspace, uint64_t user_tag,
                        uint64_t trim_seqnum) {}
    virtual void OnMetaLogApplied(const MetaLogProto& meta_log_proto) {}
    virtual void OnFinalized(uint32_t metalog_position) {} 

    Mode mode_;
    State state_;
    const View* view_;
    const View::Sequencer* sequencer_node_;
    uint32_t metalog_position_;
    std::string log_header_;

private:
    absl::flat_hash_set<size_t> interested_shards_;
    absl::FixedArray<uint32_t> shard_progrsses_;
    uint32_t seqnum_position_;

    utils::ProtobufMessagePool<MetaLogProto> metalog_pool_;
    std::vector<MetaLogProto*> applied_metalogs_;
    std::map</* metalog_seqnum */ uint32_t, MetaLogProto*> pending_metalogs_;

    void AdvanceMetaLogProgress();
    bool CanApplyMetaLog(const MetaLogProto& meta_log);
    void ApplyMetaLog(const MetaLogProto& meta_log);

    DISALLOW_COPY_AND_ASSIGN(LogSpaceBase);
};

template<class T>
class LogSpaceCollection {
public:
    LogSpaceCollection() {}
    ~LogSpaceCollection() {}

    // Return nullptr if not found
    LockablePtr<T> GetLogSpace(uint32_t identifier) const;
    // Panic if not found. It ensures the return is never nullptr
    LockablePtr<T> GetLogSpaceChecked(uint32_t identifier) const;

    void InstallLogSpace(std::unique_ptr<T> log_space);
    // Only active LogSpace can be finalized
    bool FinalizeLogSpace(uint32_t identifier);
    // Only finalized LogSpace can be removed
    bool RemoveLogSpace(uint32_t identifier);

    typedef std::function<void(/* identifier */ uint32_t, LockablePtr<T>)> IterCallback;
    void ForEachActiveLogSpace(const View* view, IterCallback cb) const;
    void ForEachActiveLogSpace(IterCallback cb) const;
    void ForEachFinalizedLogSpace(IterCallback cb) const;

private:
    std::set</* identifier */ uint32_t> active_log_spaces_;
    std::set</* identifier */ uint32_t> finalized_log_spaces_;

    absl::flat_hash_map</* identifier */ uint32_t, LockablePtr<T>> log_spaces_;

    DISALLOW_COPY_AND_ASSIGN(LogSpaceCollection);
};

// Start implementation of LogSpaceCollection

template<class T>
LockablePtr<T> LogSpaceCollection<T>::GetLogSpace(uint32_t identifier) const {
    if (log_spaces_.contains(identifier)) {
        return log_spaces_.at(identifier);
    } else {
        return LockablePtr<T>{};
    }
}

template<class T>
LockablePtr<T> LogSpaceCollection<T>::GetLogSpaceChecked(uint32_t identifier) const {
    if (!log_spaces_.contains(identifier)) {
        LOG(FATAL) << fmt::format("Cannot find LogSpace with identifier {}", identifier);
    }
    return log_spaces_.at(identifier);
}

template<class T>
void LogSpaceCollection<T>::InstallLogSpace(std::unique_ptr<T> log_space) {
    uint32_t identifier = log_space->identifier();
    DCHECK(active_log_spaces_.count(identifier) == 0);
    active_log_spaces_.insert(identifier);
    log_spaces_[identifier] = LockablePtr<T>(std::move(log_space));
}

template<class T>
bool LogSpaceCollection<T>::FinalizeLogSpace(uint32_t identifier) {
    if (active_log_spaces_.count(identifier) == 0) {
        return false;
    }
    active_log_spaces_.erase(identifier);
    DCHECK(finalized_log_spaces_.count(identifier) == 0);
    finalized_log_spaces_.insert(identifier);
    return true;
}

template<class T>
bool LogSpaceCollection<T>::RemoveLogSpace(uint32_t identifier) {
    if (finalized_log_spaces_.count(identifier) == 0) {
        return false;
    }
    finalized_log_spaces_.erase(identifier);
    DCHECK(log_spaces_.contains(identifier));
    log_spaces_.erase(identifier);
    return true;
}

template<class T>
void LogSpaceCollection<T>::ForEachActiveLogSpace(const View* view, IterCallback cb) const {
    auto iter = active_log_spaces_.lower_bound(bits::JoinTwo16(view->id(), 0));
    while (iter != active_log_spaces_.end()) {
        if (bits::HighHalf32(*iter) > view->id()) {
            break;
        }
        DCHECK(log_spaces_.contains(*iter));
        cb(*iter, log_spaces_.at(*iter));
        iter++;
    }
}

template<class T>
void LogSpaceCollection<T>::ForEachActiveLogSpace(IterCallback cb) const {
    auto iter = active_log_spaces_.begin();
    while (iter != active_log_spaces_.end()) {
        DCHECK(log_spaces_.contains(*iter));
        cb(*iter, log_spaces_.at(*iter));
        iter++;
    }
}

template<class T>
void LogSpaceCollection<T>::ForEachFinalizedLogSpace(IterCallback cb) const {
    auto iter = finalized_log_spaces_.begin();
    while (iter != finalized_log_spaces_.end()) {
        DCHECK(log_spaces_.contains(*iter));
        cb(*iter, log_spaces_.at(*iter));
        iter++;
    }
}

}  // namespace log
}  // namespace faas
