#include "log/sequencer_core.h"

#include "log/flags.h"

#define HLOG(l) LOG(l) << "SequencerCore: "
#define HVLOG(l) VLOG(l) << "SequencerCore: "

namespace faas {
namespace log {

using google::protobuf::Arena;

SequencerCore::SequencerCore()
    : local_cuts_changed_(false) {}

SequencerCore::~SequencerCore() {}

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
    SendAllFsmRecords(node_id);
    NewView();
}

void SequencerCore::OnNodeDisconnected(uint16_t node_id) {
    if (!conencted_nodes_.contains(node_id)) {
        HLOG(WARNING) << fmt::format("Node {} not in connected node set", node_id);
        return;
    }
    HLOG(INFO) << fmt::format("Node {} disconnected", node_id);
    conencted_nodes_.erase(node_id);
    NewView();
}

void SequencerCore::NewLocalCutMessage(const LocalCutMsgProto& message) {
    const Fsm::View* view = fsm_.current_view();
    if (message.view_id() < view->id()) {
        // Outdated message, can safely discard
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
        local_cuts_changed_ = true;
        HVLOG(1) << "Local cut changed";
    }
}

void SequencerCore::NewView() {
    size_t replicas = absl::GetFlag(FLAGS_slog_num_replicas);
    if (conencted_nodes_.size() < replicas) {
        HLOG(WARNING) << fmt::format("Connected nodes less than replicas {}", replicas);
        return;
    }
    FsmRecordProto* record = Arena::CreateMessage<FsmRecordProto>(&protobuf_arena_);
    Fsm::NodeVec node_vec(conencted_nodes_.begin(), conencted_nodes_.end());
    fsm_.NewView(replicas, node_vec, record);
    fsm_records_.push_back(record);
    const Fsm::View* view = fsm_.current_view();
    local_cuts_.assign(view->num_nodes() * view->replicas(), 0);
    local_cuts_changed_ = false;
    global_cuts_.assign(view->num_nodes(), 0);
    BroadcastFsmRecord(view, *record);
}

void SequencerCore::MarkAndBroadcastGlobalCut() {
    if (!local_cuts_changed_) {
        return;
    }
    const Fsm::View* view = fsm_.current_view();
    DCHECK(view != nullptr);
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
    FsmRecordProto* record = Arena::CreateMessage<FsmRecordProto>(&protobuf_arena_);
    fsm_.NewGlobalCut(cuts, record);
    local_cuts_changed_ = false;
    global_cuts_ = std::move(cuts);
    BroadcastFsmRecord(view, *record);
}

void SequencerCore::SendAllFsmRecords(uint16_t node_id) {
    FsmRecordsMsgProto message;
    for (const FsmRecordProto* record : fsm_records_) {
        message.add_records()->CopyFrom(*record);
    }
    std::string serialized_message;
    message.SerializeToString(&serialized_message);
    send_fsm_records_message_cb_(node_id, serialized_message);
}

void SequencerCore::BroadcastFsmRecord(const Fsm::View* view, const FsmRecordProto& record) {
    FsmRecordsMsgProto message;
    message.add_records()->CopyFrom(record);
    std::string serialized_message;
    message.SerializeToString(&serialized_message);
    for (size_t i = 0; i < view->num_nodes(); i++) {
        send_fsm_records_message_cb_(view->node(i), serialized_message);
    }
}

}  // namespace log
}  // namespace faas
