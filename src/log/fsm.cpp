#include "log/fsm.h"

#include "common/protocol.h"

#define HLOG(l) LOG(l) << "LogFsm: "
#define HVLOG(l) VLOG(l) << "LogFsm: "

namespace faas {
namespace log {

Fsm::Fsm()
    : new_view_cb_([] (const View*) {}),
      log_replicated_cb_([] (uint64_t, uint64_t, uint32_t) {}),
      next_record_seqnum_(0),
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

void Fsm::NewView(size_t replicas, const NodeVec& nodes, FsmRecordProto* record) {
    record->Clear();
    record->set_seqnum(next_record_seqnum_);
    record->set_type(FsmRecordType::NEW_VIEW);
    NewViewRecordProto* new_view_record = record->mutable_new_view_record();
    new_view_record->set_view_id(next_view_id());
    new_view_record->set_replicas(gsl::narrow_cast<uint32_t>(replicas));
    for (const auto& node : nodes) {
        NodeProto* node_proto = new_view_record->add_nodes();
        node_proto->set_id(node.first);
        node_proto->set_addr(node.second);
    }
    ApplyRecord(*record);
}

void Fsm::NewGlobalCut(const CutVec& cuts, FsmRecordProto* record) {
    record->Clear();
    record->set_seqnum(next_record_seqnum_);
    record->set_type(FsmRecordType::GLOBAL_CUT);
    GlobalCutRecordProto* global_cut_record = record->mutable_global_cut_record();
    global_cut_record->set_start_seqnum(next_log_seqnum_);
    global_cut_record->set_end_seqnum(protocol::kInvalidLogSeqNum);
    for (size_t i = 0; i < cuts.size(); i++) {
        global_cut_record->add_localid_cuts(cuts[i]);
    }
    ApplyRecord(*record);
    global_cut_record->set_end_seqnum(next_log_seqnum_);
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
    if (view->id() != next_view_id()) {
        HLOG(FATAL) << "View ID not increasing!";
    }
    HLOG(INFO) << "Start new view with id " << view->id();
    views_.emplace_back(view);
    next_log_seqnum_ = BuildSeqNum(view->id(), 0);
    new_view_cb_(view);
}

void Fsm::ApplyGlobalCutRecord(const GlobalCutRecordProto& record) {
    const View* view = current_view();
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
        if (start_localid < end_localid) {
            log_replicated_cb_(
                /* start_localid= */ BuildLocalId(view->id(), node_id, start_localid),
                /* start_seqnum= */  next_log_seqnum_,
                /* delta= */         end_localid - start_localid
            );
            next_log_seqnum_ += end_localid - start_localid;
        } else if (start_localid > end_localid) {
            HLOG(FATAL) << "localid_cuts from GlobalCutRecordProto not increasing";
        }
    }
    if (record.end_seqnum() != protocol::kInvalidLogSeqNum
            && next_log_seqnum_ != record.end_seqnum()) {
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

void Fsm::View::ForEachBackupNode(uint16_t primary_node_id,
                                  std::function<void(uint16_t /* node_id */)> cb) const {
    size_t base = node_indices_.at(primary_node_id);
    for (size_t i = 1; i < replicas_; i++) {
        size_t node_id = node_ids_[(base + i) % node_ids_.size()];
        cb(node_id);
    }
}

void Fsm::View::ForEachPrimaryNode(uint16_t backup_node_id,
                                   std::function<void(uint16_t /* node_id */)> cb) const {
    size_t base = node_indices_.at(backup_node_id);
    for (size_t i = 1; i < replicas_; i++) {
        size_t node_id = node_ids_[(base - i + node_ids_.size()) % node_ids_.size()];
        cb(node_id);
    }
}

}  // namespace log
}  // namespace faas
