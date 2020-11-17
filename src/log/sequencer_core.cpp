#include "log/sequencer_core.h"

#include "log/flags.h"

#define HLOG(l) LOG(l) << "SequencerCore: "
#define HVLOG(l) VLOG(l) << "SequencerCore: "

namespace faas {
namespace log {

using google::protobuf::Arena;

SequencerCore::SequencerCore() {}

SequencerCore::~SequencerCore() {}

void SequencerCore::SetSendFsmRecordsMessageCallback(SendFsmRecordsMessageCallback cb) {
    send_fsm_records_message_cb_ = cb;
}

int SequencerCore::global_cut_interval_us() const {
    return absl::GetFlag(FLAGS_slog_global_cut_interval_us);
}

void SequencerCore::MarkAndBroadcastGlobalCut() {
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
    }
}

void SequencerCore::NewView() {
    FsmRecordProto* record = Arena::CreateMessage<FsmRecordProto>(&protobuf_arena_);
    Fsm::NodeVec node_vec(conencted_nodes_.begin(), conencted_nodes_.end());
    fsm_.NewView(absl::GetFlag(FLAGS_slog_num_replicas), node_vec, record);
    fsm_records_.push_back(record);
    const Fsm::View* view = fsm_.current_view();
    local_cuts_.assign(view->num_nodes() * view->replicas(), 0);
    BroadcastFsmRecord(view, *record);
}

void SequencerCore::NewGlobalCut() {
    // TODO
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
