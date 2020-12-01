#pragma once

#include "log/common.h"

namespace faas {
namespace log {

class Fsm {
public:
    explicit Fsm(uint16_t sequencer_id = 0);
    ~Fsm();

    class View;

    typedef std::function<void(const View*)> NewViewCallback;
    void SetNewViewCallback(NewViewCallback cb);

    typedef std::function<void(uint64_t /* start_localid */, uint64_t /* start_seqnum */,
                               uint32_t /* delta */)>
            LogReplicatedCallback;
    void SetLogReplicatedCallback(LogReplicatedCallback cb);

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

    std::string get_addr(uint16_t node_id) const {
        if (!node_addr_.contains(node_id)) {
            return "";
        }
        return node_addr_.at(node_id);
    }

    // Find the first log entry whose `seqnum` >= `start_seqnum`
    // Will return the seqnum, associated view and primary node
    bool FindNextSeqnum(uint64_t start_seqnum, uint64_t* seqnum,
                        const View** view, uint16_t* primary_node_id) const;

    // Find the `seqnum` of the tailing log entry, associated view and primary node
    bool CheckTail(uint64_t* seqnum, const View** view, uint16_t* primary_node_id) const;

    // ApplyRecord only works if records are applied in strict order
    // Used in sequencers, where Raft guarantees the order 
    void ApplyRecord(const FsmRecordProto& record);

    // OnRecvRecord can handle out-of-order receiving of records
    // Used in engines, where records are received from sequencers
    void OnRecvRecord(const FsmRecordProto& record);

    typedef std::vector<std::pair</* node_id */ uint16_t, /* addr */ std::string>> NodeVec;
    void BuildNewViewRecord(size_t replicas, const NodeVec& nodes, FsmRecordProto* record);

    typedef std::vector<uint32_t> CutVec;
    void BuildGlobalCutRecord(const CutVec& cuts, FsmRecordProto* record);

private:
    uint16_t sequencer_id_;

    NewViewCallback       new_view_cb_;
    LogReplicatedCallback log_replicated_cb_;

    uint32_t next_record_seqnum_;
    absl::flat_hash_map</* seqnum */ uint32_t, FsmRecordProto> pending_records_;
    uint64_t next_log_seqnum_;

    std::vector<std::unique_ptr<View>> views_;
    absl::flat_hash_map</* node_id */ uint16_t, std::string> node_addr_;

    struct GlobalCut {
        uint16_t view_id;
        uint64_t start_seqnum;
        uint64_t end_seqnum;
        std::vector<uint32_t> localid_cuts;
        std::vector<uint32_t> deltas;
    };
    std::vector<std::unique_ptr<GlobalCut>> global_cuts_;

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

    std::string_view get_addr(uint16_t node_id) const {
        return node_addr_.at(node_id);
    }

    void ForEachBackupNode(uint16_t primary_node_id,
                           std::function<void(uint16_t /* node_id */)> cb) const;
    void ForEachPrimaryNode(uint16_t backup_node_id,
                            std::function<void(uint16_t /* node_id */)> cb) const;

    bool IsStorageNodeOf(uint16_t primary_node_id, uint16_t node_id) const;
    uint16_t PickOneStorageNode(uint16_t primary_node_id) const;

    uint16_t LogTagToPrimaryNode(uint32_t log_tag) const;

private:
    uint16_t id_;
    size_t replicas_;
    std::vector<uint16_t> node_ids_;
    absl::flat_hash_map</* node_id */ uint16_t, size_t> node_indices_;
    absl::flat_hash_map</* node_id */ uint16_t, std::string> node_addr_;

    DISALLOW_COPY_AND_ASSIGN(View);
};

}  // namespace log
}  // namespace faas
