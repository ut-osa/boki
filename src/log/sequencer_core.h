#pragma once

#include "common/stat.h"
#include "utils/object_pool.h"
#include "log/common.h"
#include "log/fsm.h"

namespace faas {
namespace log {

class SequencerCore {
public:
    explicit SequencerCore(uint16_t sequencer_id);
    ~SequencerCore();

    typedef std::function<bool(uint16_t* /* leader_id */)> RaftLeaderCallback;
    void SetRaftLeaderCallback(RaftLeaderCallback cb);

    typedef std::function<uint64_t()> RaftCurrentTermCallback;
    void SetRaftCurrentTermCallback(RaftCurrentTermCallback cb);

    typedef std::function<void(uint32_t /* seqnum */,
                               std::span<const char> /* payload */)> RaftApplyCallback;
    void SetRaftApplyCallback(RaftApplyCallback cb);

    typedef std::function<void(uint64_t /* data */)> RaftBarrierCallback;
    void SetRaftBarrierCallback(RaftBarrierCallback cb);

    typedef std::function<void(uint16_t /* node_id */, std::span<const char> /* data */)>
            SendFsmRecordsMessageCallback;
    void SetSendFsmRecordsMessageCallback(SendFsmRecordsMessageCallback cb);

    int global_cut_interval_us() const;
    void MarkGlobalCutIfNeeded();
    void DoViewChecking();
    void ReconfigView();

    void OnNewNodeConnected(uint16_t node_id, std::string_view addr);
    void OnNodeDisconnected(uint16_t node_id);
    void OnRecvLocalCutMessage(const LocalCutMsgProto& message);
    void OnRaftApplyFinished(uint32_t seqnum, bool success);
    void OnRaftBarrierFinished(uint64_t data, bool success);

    bool RaftFsmApplyCallback(std::span<const char> payload);
    bool RaftFsmRestoreCallback(std::span<const char> payload);
    bool RaftFsmSnapshotCallback(std::string* data);

    void DoStateCheck(std::ostringstream& stream) const;

private:
    uint16_t sequencer_id_;
    Fsm fsm_;
    std::vector<FsmRecordProto*> fsm_records_;
    FsmRecordProto* ongoing_record_;
    utils::ProtobufMessagePool<FsmRecordProto> fsm_record_pool_;
    uint64_t consolidated_raft_term_;

    RaftLeaderCallback            raft_leader_cb_;
    RaftCurrentTermCallback       raft_current_term_cb_;
    RaftApplyCallback             raft_apply_cb_;
    RaftBarrierCallback           raft_barrier_cb_;
    SendFsmRecordsMessageCallback send_fsm_records_message_cb_;

    absl::flat_hash_map</* node_id */ uint16_t, /* addr */ std::string>
        connected_nodes_;

    std::vector<uint32_t> local_cuts_;
    std::vector<uint32_t> global_cuts_;

    stat::StatisticsCollector<uint32_t> global_cut_delta_stat_; 

    bool new_view_pending_;

    bool IsRaftLeader();
    bool ConsolidateMyRaftTerm();

    void NewView();

    void RaftApplyRecord(FsmRecordProto* record);
    void ApplyNewViewRecord(FsmRecordProto* record);
    void ApplyGlobalCutRecord(FsmRecordProto* record);

    void SendAllFsmRecords(uint16_t node_id);
    void BroadcastFsmRecord(const Fsm::View* view, const FsmRecordProto& record);

    DISALLOW_COPY_AND_ASSIGN(SequencerCore);
};

}  // namespace log
}  // namespace faas
