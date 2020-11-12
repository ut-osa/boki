#include "log/fsm.h"

#define HLOG(l) LOG(l) << "LogFsm: "
#define HVLOG(l) VLOG(l) << "LogFsm: "

namespace faas {
namespace log {

Fsm::Fsm()
    : next_record_seqnum_(0),
      next_log_seqnum_(0) {}

Fsm::~Fsm() {}

void Fsm::SetNewViewCallback(NewViewCallback cb) {
    new_view_cb_ = cb;
}

void Fsm::SetLogReplicatedCallback(LogReplicatedCallback cb) {
    log_replicated_cb_ = cb;
}

void Fsm::OnRecvRecord(const FsmRecordProto& record) {
    if (record.seqnum() == next_record_seqnum_) {
        ApplyRecord(record);
        while (pending_records_.contains(next_record_seqnum_)) {
            ApplyRecord(pending_records_[next_record_seqnum_]);
            pending_records_.erase(next_record_seqnum_);
        }
    } else {
        pending_records_[record.seqnum()] = record;
    }
}

void Fsm::ApplyRecord(const FsmRecordProto& record) {
    DCHECK_EQ(record.seqnum(), next_record_seqnum_);
    next_record_seqnum_++;
    HVLOG(1) << fmt::format("Fsm::ApplyRecord: seqnum={}", record.seqnum());
    switch (record.type()) {
    case FsmRecordType::NEW_VIEW:
        ApplyNewViewRecord(record.new_view_record());
        break;
    case FsmRecordType::GLOBAL_CUT:
        ApplyGlobalCutRecord(record.global_cut_record());
        break;
    default:
        HLOG(FATAL) << "Unknown record type";
    }
}

void Fsm::ApplyNewViewRecord(const NewViewRecordProto& record) {
    View* view = new View(record);
    views_.emplace_back(view);
    next_log_seqnum_ = BuildSeqNum(view->id(), 0);
    new_view_cb_(view);
}

void Fsm::ApplyGlobalCutRecord(const GlobalCutRecordProto& record) {
    View* view = current_view();
    DCHECK(view != nullptr);
    if (record.start_seqnum() != next_log_seqnum_) {
        HLOG(FATAL) << "Inconsistent start_seqnum from GlobalCutRecordProto";
    }
    if (gsl::narrow_cast<size_t>(record.localid_cuts_size()) != view->num_nodes()) {
        HLOG(FATAL) << "Inconsistent size of localid_cuts from GlobalCutRecordProto";
    }
    GlobalCut* previous_cut = nullptr;
    if (!global_cuts_.empty() && global_cuts_.back()->view_id == view->id()) {
        previous_cut = global_cuts_.back().get();
    }
    GlobalCut* new_cut = new GlobalCut;
    new_cut->view_id = view->id();
    new_cut->start_seqnum = record.start_seqnum();
    new_cut->localid_cuts.assign(record.localid_cuts().begin(), record.localid_cuts().end());
    global_cuts_.emplace_back(new_cut);
    for (size_t i = 0; i < view->num_nodes(); i++) {
        uint16_t node_id = view->node(i);
        uint32_t start_localid = previous_cut == nullptr ? 0 : previous_cut->localid_cuts[i];
        uint32_t end_localid = new_cut->localid_cuts[i];
        if (start_localid >= end_localid) {
            HLOG(FATAL) << "localid_cuts from GlobalCutRecordProto not increasing";
        }
        uint64_t start_seqnum = next_log_seqnum_;
        uint64_t end_seqnum = start_seqnum + (end_localid - start_localid);
        log_replicated_cb_(std::make_pair(BuildLocalId(view->id(), node_id, start_localid),
                                          BuildLocalId(view->id(), node_id, end_localid)),
                           std::make_pair(start_seqnum, end_seqnum));
        next_log_seqnum_ = end_seqnum;
    }
    if (next_log_seqnum_ != record.end_seqnum()) {
        HLOG(FATAL) << "Inconsistent end_seqnum from GlobalCutRecordProto";
    }
}

Fsm::View::View(const NewViewRecordProto& proto)
    : id_(proto.view_id()),
      replicas_(proto.replicas()) {
    for (const NodeProto& node : proto.nodes()) {
        size_t idx = node_ids_.size();
        uint16_t node_id = gsl::narrow_cast<uint16_t>(node.id());
        node_ids_.push_back(node_id);
        node_indices_[node_id] = idx;
        node_addr_[node_id] = node.addr();
    }
}

Fsm::View::~View() {}

void Fsm::View::ForEachBackNode(uint16_t primary_node_id,
                           std::function<void(uint16_t /* node_id */)> cb) const {
    size_t base = node_indices_.at(primary_node_id);
    for (size_t i = 1; i < replicas_; i++) {
        size_t node_id = node_ids_[(base + i) % node_ids_.size()];
        cb(node_id);
    }
}

}  // namespace log
}  // namespace faas
