#pragma once

#include "common/stat.h"
#include "log/common.h"

namespace faas {
namespace log {

class Fsm {
public:
    explicit Fsm(uint16_t sequencer_id = 0);
    ~Fsm();

    class View;

    typedef std::function<void(uint32_t /* record_seqnum */, const View*)> NewViewCallback;
    void SetNewViewCallback(NewViewCallback cb);

    typedef std::function<void(uint64_t /* start_localid */, uint64_t /* start_seqnum */,
                               uint32_t /* delta */)>
            LogReplicatedCallback;
    void SetLogReplicatedCallback(LogReplicatedCallback cb);

    typedef std::function<void(uint32_t /* record_seqnum */, uint64_t /* start_seqnum */,
                               uint64_t /* end_seqnum */)> GlobalCutCallback;
    void SetGlobalCutCallback(GlobalCutCallback cb);

    const View* current_view() const {
        return views_.empty() ? nullptr : views_.back().get();
    }

    const View* view_with_id(uint16_t view_id) const {
        if (view_id < views_.size()) {
            return views_.at(view_id).get();
        } else {
            return nullptr;
        }
    }

    uint16_t next_view_id() const {
        return gsl::narrow_cast<uint16_t>(views_.size());
    }

    uint32_t progress() const { return next_record_seqnum_; }

    std::string get_addr(uint16_t node_id) const {
        if (!node_addr_.contains(node_id)) {
            return "";
        }
        return node_addr_.at(node_id);
    }

    // Convert given localid to seqnum, based on known FSM records.
    // Return true if we can figure out.
    // `seqnum` will be set to kInvalidLogSeqNum if given localid is invalid,
    // or known to be discarded.
    bool ConvertLocalId(uint64_t localid, uint64_t* seqnum) const;

    // Find the first log entry whose `seqnum` >= `ref_seqnum`
    // Will return the seqnum, associated view and primary node
    bool FindNextSeqnum(uint64_t ref_seqnum, uint64_t* seqnum,
                        const View** view, uint16_t* primary_node_id) const;

    // Find the last log entry whose `seqnum` <= `ref_seqnum`
    // Will return the seqnum, associated view and primary node
    bool FindPrevSeqnum(uint64_t ref_seqnum, uint64_t* seqnum,
                        const View** view, uint16_t* primary_node_id) const;

    // ApplyRecord only works if records are applied in strict order
    // Used in sequencers, where Raft guarantees the order 
    void ApplyRecord(const FsmRecordProto& record);

    // OnRecvRecord can handle out-of-order receiving of records
    // Used in engines, where records are received from sequencers
    void OnRecvRecord(const FsmRecordProto& record);

    typedef std::vector<std::pair</* node_id */ uint16_t, /* addr */ std::string>> NodeVec;
    void BuildNewViewRecord(size_t replicas, const NodeVec& nodes, FsmRecordProto* record);

    typedef absl::FixedArray<uint32_t> CutVec;
    void BuildGlobalCutRecord(const CutVec& cuts, FsmRecordProto* record);

    void DoStateCheck(std::ostringstream& stream) const;

private:
    uint16_t sequencer_id_;

    NewViewCallback       new_view_cb_;
    LogReplicatedCallback log_replicated_cb_;
    GlobalCutCallback     global_cut_cb_;

    uint32_t next_record_seqnum_;
    absl::flat_hash_map</* seqnum */ uint32_t, FsmRecordProto> pending_records_;
    stat::Counter record_apply_counter_;
    stat::StatisticsCollector<uint32_t> pending_records_stat_; 

    uint64_t next_log_seqnum_;

    std::vector<std::unique_ptr<View>> views_;
    std::vector<size_t> first_cut_of_view_;
    absl::flat_hash_map</* node_id */ uint16_t, std::string> node_addr_;

    struct GlobalCut {
        const View* view;
        uint64_t start_seqnum;
        uint64_t end_seqnum;
        absl::FixedArray<uint32_t> localid_cuts;
        absl::FixedArray<uint32_t> deltas;

        explicit GlobalCut(const View* view);
    };
    std::vector<std::unique_ptr<GlobalCut>> global_cuts_;

    static uint16_t LocatePrimaryNode(const GlobalCut& cut, uint64_t seqnum);
    static uint64_t BuildSeqNumFromCounter(const GlobalCut& cut, uint16_t node_idx,
                                           uint32_t counter);

    void ApplyNewViewRecord(const NewViewRecordProto& record);
    void ApplyGlobalCutRecord(const GlobalCutRecordProto& record);

    DISALLOW_COPY_AND_ASSIGN(Fsm);
};

class Fsm::View {
public:
    explicit View(const NewViewRecordProto& proto);
    ~View();

    uint16_t id() const { return id_; }
    size_t replicas() const { return replicas_; }

    size_t num_nodes() const { return node_ids_.size(); }
    uint16_t node(size_t idx) const { return node_ids_[idx]; }

    bool has_node(uint16_t node_id) const {
        return node_indices_.contains(node_id);
    }

    size_t node_idx(uint16_t node_id) const {
        DCHECK(node_indices_.contains(node_id));
        return node_indices_.at(node_id);
    }

    std::string_view get_addr(uint16_t node_id) const {
        DCHECK(node_addr_.contains(node_id));
        return node_addr_.at(node_id);
    }

    void ForEachNode(std::function<void(size_t /* idx */, uint16_t /* node_id */)> cb) const;
    void ForEachBackupNode(uint16_t primary_node_id,
                           std::function<void(uint16_t /* node_id */)> cb) const;
    void ForEachPrimaryNode(uint16_t backup_node_id,
                            std::function<void(uint16_t /* node_id */)> cb) const;

    uint16_t PickOneNode() const;
    bool IsStorageNodeOf(uint16_t primary_node_id, uint16_t node_id) const;
    uint16_t PickOneStorageNode(uint16_t primary_node_id) const;

    uint16_t LogTagToPrimaryNode(uint64_t log_tag) const;

private:
    uint16_t id_;
    size_t replicas_;
    absl::FixedArray<uint16_t> node_ids_;
    absl::flat_hash_map</* node_id */ uint16_t, size_t> node_indices_;
    absl::flat_hash_map</* node_id */ uint16_t, std::string> node_addr_;
    uint64_t hash_seed_;

    mutable absl::FixedArray<std::atomic<size_t>> next_storage_node_;

    void ComputeHashSeed();

    DISALLOW_COPY_AND_ASSIGN(View);
};

}  // namespace log
}  // namespace faas
