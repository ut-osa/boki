#include "log/sequencer_core.h"

#include "log/flags.h"

#define log_header_ "SequencerCore: "

namespace faas {
namespace log {

using google::protobuf::Arena;

SequencerCore::SequencerCore(uint16_t sequencer_id)
    : sequencer_id_(sequencer_id),
      fsm_(sequencer_id),
      ongoing_record_(nullptr),
      new_view_pending_(false) {}

SequencerCore::~SequencerCore() {}

void SequencerCore::SetRaftLeaderCallback(RaftLeaderCallback cb) {
    raft_leader_cb_ = cb;
}

void SequencerCore::SetRaftApplyCallback(RaftApplyCallback cb) {
    raft_apply_cb_ = cb;
}

void SequencerCore::SetSendFsmRecordsMessageCallback(SendFsmRecordsMessageCallback cb) {
    send_fsm_records_message_cb_ = cb;
}

int SequencerCore::global_cut_interval_us() const {
    return absl::GetFlag(FLAGS_slog_global_cut_interval_us);
}

void SequencerCore::OnNewNodeConnected(uint16_t node_id, std::string_view addr) {
    if (conencted_nodes_.contains(node_id)) {
        HLOG(WARNING) << fmt::format("Node {} already in connected node set", node_id);
        return;
    }
    HLOG(INFO) << fmt::format("Node {} connected with address {}", node_id, addr);
    conencted_nodes_[node_id] = std::string(addr);
    // This can be problematic, as the new node will not receive new records
    // before it becomes part of the new view.
    // TODO: Find some way to trigger it, and fix the problem 
    SendAllFsmRecords(node_id);
    ReconfigViewIfDoable();
}

void SequencerCore::OnNodeDisconnected(uint16_t node_id) {
    if (!conencted_nodes_.contains(node_id)) {
        HLOG(WARNING) << fmt::format("Node {} not in connected node set", node_id);
        return;
    }
    HLOG(INFO) << fmt::format("Node {} disconnected", node_id);
    conencted_nodes_.erase(node_id);
    ReconfigViewIfDoable();
}

void SequencerCore::OnRecvLocalCutMessage(const LocalCutMsgProto& message) {
    const Fsm::View* view = fsm_.current_view();
    if (message.view_id() < view->id()) {
        // Outdated message, can safely discard
        HLOG(WARNING) << "Outdated local cut message";
        return;
    }
    if (message.view_id() > view->id()) {
        HLOG(ERROR) << "view_id in received LocalCutMessage is larger than current view";
        return;
    }
    if (gsl::narrow_cast<size_t>(message.localid_cuts_size()) != view->replicas()) {
        HLOG(ERROR) << "Size of localid_cuts in received LocalCutMessage "
                       "does not match the number of replicas";
        return;
    }
    size_t node_idx = 0;
    while (node_idx < view->num_nodes() && view->node(node_idx) != message.my_node_id()) {
        node_idx++;
    }
    if (node_idx == view->num_nodes()) {
        HLOG(ERROR) << fmt::format("Node {} does not exist in the current view",
                                   message.my_node_id());
        return;
    }
    bool need_update = false;
    for (size_t i = 0; i < view->replicas(); i++) {
        size_t idx = node_idx * view->replicas() + i;
        if (message.localid_cuts(i) > local_cuts_[idx]) {
            need_update = true;
        }
        if (need_update && message.localid_cuts(i) < local_cuts_[idx]) {
            HLOG(ERROR) << "localid_cuts in received LocalCutMessage inconsistent with "
                           "current local cut";
            return;
        }
    }
    if (need_update) {
        for (size_t i = 0; i < view->replicas(); i++) {
            local_cuts_[node_idx * view->replicas() + i] = message.localid_cuts(i);
        }
        HVLOG(1) << "Local cut changed";
    }
}

void SequencerCore::ReconfigViewIfDoable() {
    if (!is_raft_leader() || new_view_pending_) {
        return;
    }
    HLOG(INFO) << "Will reconfigure view";
    if (has_ongoing_fsm_record()) {
        new_view_pending_ = true;
    } else {
        NewView();
    }
}

void SequencerCore::NewView() {
    DCHECK(is_raft_leader());
    DCHECK(!has_ongoing_fsm_record());
    new_view_pending_ = false;
    size_t replicas = absl::GetFlag(FLAGS_slog_num_replicas);
    if (conencted_nodes_.size() < replicas) {
        HLOG(WARNING) << fmt::format("Connected nodes less than replicas {}", replicas);
        return;
    }
    HLOG(INFO) << "Create new view";
    FsmRecordProto* record = fsm_record_pool_.Get();
    Fsm::NodeVec node_vec(conencted_nodes_.begin(), conencted_nodes_.end());
    std::random_shuffle(node_vec.begin(), node_vec.end());
    fsm_.BuildNewViewRecord(replicas, node_vec, record);
    RaftApplyRecord(record);
}

void SequencerCore::MarkGlobalCutIfNeeded() {
    if (!is_raft_leader() || has_ongoing_fsm_record() || new_view_pending_) {
        return;
    }
    const Fsm::View* view = fsm_.current_view();
    if (view == nullptr) {
        return;
    }
    std::vector<uint32_t> cuts(view->num_nodes(), std::numeric_limits<uint32_t>::max());
    for (size_t i = 0; i < view->num_nodes(); i++) {
        for (size_t j = 0; j < view->replicas(); j++) {
            size_t node_idx = (i - j + view->num_nodes()) % view->num_nodes();
            uint32_t value = local_cuts_[i * view->replicas() + j];
            cuts[node_idx] = std::min(cuts[node_idx], value);
        }
    }
    DCHECK_EQ(view->num_nodes(), global_cuts_.size());
    bool changed = false;
    for (size_t i = 0; i < view->num_nodes(); i++) {
        DCHECK_GE(cuts[i], global_cuts_[i]);
        if (cuts[i] > global_cuts_[i]) {
            changed = true;
            break;
        }
    }
    if (!changed) {
        return;
    }
    HVLOG(1) << "Apply and broadcast new global cut";
    FsmRecordProto* record = fsm_record_pool_.Get();
    fsm_.BuildGlobalCutRecord(cuts, record);
    RaftApplyRecord(record);
}

bool SequencerCore::is_raft_leader() {
    uint16_t leader_id;
    if (!raft_leader_cb_(&leader_id)) {
        return false;
    }
    return leader_id == sequencer_id_;
}

void SequencerCore::SendAllFsmRecords(uint16_t node_id) {
    if (!is_raft_leader()) {
        // Well, now only the leader is responsible for broadcasting records
        return;
    }
    FsmRecordsMsgProto message;
    for (const FsmRecordProto* record : fsm_records_) {
        message.add_records()->CopyFrom(*record);
    }
    std::string serialized_message;
    message.SerializeToString(&serialized_message);
    send_fsm_records_message_cb_(node_id, serialized_message);
}

void SequencerCore::BroadcastFsmRecord(const Fsm::View* view, const FsmRecordProto& record) {
    if (!is_raft_leader()) {
        // Well, now only the leader is responsible for broadcasting records
        return;
    }
    FsmRecordsMsgProto message;
    message.add_records()->CopyFrom(record);
    std::string serialized_message;
    message.SerializeToString(&serialized_message);
    for (size_t i = 0; i < view->num_nodes(); i++) {
        send_fsm_records_message_cb_(view->node(i), serialized_message);
    }
}

void SequencerCore::RaftApplyRecord(FsmRecordProto* record) {
    record->set_seqnum(gsl::narrow_cast<uint32_t>(fsm_records_.size()));
    ongoing_record_ = record;
    std::string serialized_record;
    record->SerializeToString(&serialized_record);
    std::span<const char> payload(serialized_record.data(), serialized_record.size());
    raft_apply_cb_(record->seqnum(), payload);
}

void SequencerCore::OnRaftApplyFinished(uint32_t seqnum, bool success) {
    DCHECK(ongoing_record_ != nullptr);
    DCHECK_EQ(ongoing_record_->seqnum(), seqnum);
    fsm_record_pool_.Return(ongoing_record_);
    ongoing_record_ = nullptr;
    DCHECK(!has_ongoing_fsm_record());
    if (is_raft_leader() && new_view_pending_) {
        NewView();  // will clear new_view_pending_
    }
}

void SequencerCore::ApplyNewViewRecord(FsmRecordProto* record) {
    fsm_.ApplyRecord(*record);
    const NewViewRecordProto& new_view_record = record->new_view_record();
    const Fsm::View* view = fsm_.current_view();
    DCHECK_EQ(view->id(), gsl::narrow_cast<uint16_t>(new_view_record.view_id()));
    local_cuts_.assign(view->num_nodes() * view->replicas(), 0);
    global_cuts_.assign(view->num_nodes(), 0);
    BroadcastFsmRecord(view, *record);
}

void SequencerCore::ApplyGlobalCutRecord(FsmRecordProto* record) {
    fsm_.ApplyRecord(*record);
    const GlobalCutRecordProto& global_cut_record = record->global_cut_record();
    const Fsm::View* view = fsm_.current_view();
    DCHECK_EQ(view->id(), SeqNumToViewId(global_cut_record.start_seqnum()));
    DCHECK_EQ(view->num_nodes(), gsl::narrow_cast<size_t>(global_cut_record.localid_cuts_size()));
    DCHECK_EQ(view->num_nodes(), global_cuts_.size());
    for (size_t i = 0; i < view->num_nodes(); i++) {
        global_cuts_[i] = global_cut_record.localid_cuts(i);
    }
    BroadcastFsmRecord(view, *record);
}

bool SequencerCore::RaftFsmApplyCallback(std::span<const char> payload) {
    HVLOG(1) << "RaftFsmApplyCallback";
    FsmRecordProto* record = fsm_record_pool_.Get();
    if (!record->ParseFromArray(payload.data(), payload.size())) {
        HLOG(ERROR) << "Failed to parse new Fsm record";
        fsm_record_pool_.Return(record);
        return false;
    }
    if (record->seqnum() != fsm_records_.size()) {
        HLOG(ERROR) << "Inconsistent seqnum from the new record";
        fsm_record_pool_.Return(record);
        return false;
    }
    fsm_records_.push_back(record);
    switch (record->type()) {
    case FsmRecordType::NEW_VIEW:
        ApplyNewViewRecord(record);
        break;
    case FsmRecordType::GLOBAL_CUT:
        ApplyGlobalCutRecord(record);
        break;
    default:
        HLOG(FATAL) << "Unknown record type";
    }
    return true;
}

bool SequencerCore::RaftFsmRestoreCallback(std::span<const char> payload) {
    NOT_IMPLEMENTED();
}

bool SequencerCore::RaftFsmSnapshotCallback(std::string* data) {
    NOT_IMPLEMENTED();
}

}  // namespace log
}  // namespace faas
