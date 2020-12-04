#include "log/fsm.h"

#include "common/protocol.h"
#include "utils/random.h"
#include "utils/hash.h"

#define log_header_ "LogFsm: "

namespace faas {
namespace log {

Fsm::Fsm(uint16_t sequencer_id)
    : sequencer_id_(sequencer_id),
      new_view_cb_([] (const View*) {}),
      log_replicated_cb_([] (uint64_t, uint64_t, uint32_t) {}),
      next_record_seqnum_(0),
      next_log_seqnum_(0) {}

Fsm::~Fsm() {}

Fsm::GlobalCut::GlobalCut(const View* view)
    : localid_cuts(view->num_nodes()),
      deltas(view->num_nodes(), 0) {
    this->view = view;
}

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

void Fsm::BuildNewViewRecord(size_t replicas, const NodeVec& nodes, FsmRecordProto* record) {
    record->Clear();
    record->set_seqnum(next_record_seqnum_);
    record->set_sequencer_id(sequencer_id_);
    record->set_type(FsmRecordType::NEW_VIEW);
    NewViewRecordProto* new_view_record = record->mutable_new_view_record();
    new_view_record->set_view_id(next_view_id());
    new_view_record->set_replicas(gsl::narrow_cast<uint32_t>(replicas));
    for (const auto& node : nodes) {
        NodeProto* node_proto = new_view_record->add_nodes();
        node_proto->set_id(node.first);
        node_proto->set_addr(node.second);
    }
}

void Fsm::BuildGlobalCutRecord(const CutVec& cuts, FsmRecordProto* record) {
    record->Clear();
    record->set_seqnum(next_record_seqnum_);
    record->set_sequencer_id(sequencer_id_);
    record->set_type(FsmRecordType::GLOBAL_CUT);
    GlobalCutRecordProto* global_cut_record = record->mutable_global_cut_record();
    global_cut_record->set_start_seqnum(next_log_seqnum_);
    for (size_t i = 0; i < cuts.size(); i++) {
        global_cut_record->add_localid_cuts(cuts[i]);
    }
}

bool Fsm::FindNextSeqnum(uint64_t start_seqnum, uint64_t* seqnum,
                         const View** view, uint16_t* primary_node_id) const {
    if (global_cuts_.empty()) {
        return false;
    }
    size_t left = 0;
    size_t right = global_cuts_.size();
    while (left < right) {
        size_t mid = (left + right) / 2;
        DCHECK_LT(mid, global_cuts_.size());
        if (start_seqnum >= global_cuts_.at(mid)->end_seqnum) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    if (right >= global_cuts_.size()) {
        return false;
    }
    size_t pos = right;
    DCHECK(start_seqnum < global_cuts_.at(pos)->end_seqnum);
    DCHECK(pos == 0 || start_seqnum >= global_cuts_.at(pos-1)->end_seqnum);
    const GlobalCut* target_cut = global_cuts_.at(pos).get();
    const View* target_view = target_cut->view;
    if (target_cut->start_seqnum < start_seqnum) {
        uint64_t current_seqnum = target_cut->start_seqnum;
        size_t node_idx = 0;
        while (node_idx < target_view->num_nodes()) {
            uint32_t delta = target_cut->deltas.at(node_idx);
            if (current_seqnum <= start_seqnum && start_seqnum < current_seqnum + delta) {
                *seqnum = start_seqnum;
                *primary_node_id = target_view->node(node_idx);
                break;
            }
            current_seqnum += delta;
            node_idx++;
        }
        DCHECK(node_idx < target_view->num_nodes());
    } else {
        *seqnum = target_cut->start_seqnum;
        size_t node_idx = 0;
        while (node_idx < target_view->num_nodes()) {
            uint32_t delta = target_cut->deltas.at(node_idx);
            if (delta > 0) {
                *primary_node_id = target_view->node(node_idx);
                break;
            }
            node_idx++;
        }
        DCHECK(node_idx < target_view->num_nodes());
    }
    *view = target_view;
    return true;
}

bool Fsm::CheckTail(uint64_t* seqnum, const View** view, uint16_t* primary_node_id) const {
    if (global_cuts_.empty()) {
        return false;
    }
    const GlobalCut* target_cut = global_cuts_.back().get();
    const View* target_view = target_cut->view;
    *view = target_view;
    *seqnum = target_cut->end_seqnum - 1;
    uint16_t last_node_id;
    for (size_t i = 0; i < target_view->num_nodes(); i++) {
        if (target_cut->deltas.at(i) > 0) {
            last_node_id = target_view->node(i);
        }
    }
    *primary_node_id = last_node_id;
    return true;
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
    for (size_t i = 0; i < view->num_nodes(); i++) {
        uint16_t node_id = view->node(i);
        std::string_view addr = view->get_addr(node_id);
        if (node_addr_.contains(node_id)) {
            if (addr != node_addr_[node_id]) {
                HLOG(FATAL) << fmt::format("Node {} has inconsistent address: {} and {}",
                                           node_id, node_addr_[node_id], addr);
            }
        } else {
            HLOG(INFO) << fmt::format("Seen new node {} with address {}", node_id, addr);
            node_addr_[node_id] = std::string(addr);
        }
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
    if (!global_cuts_.empty() && global_cuts_.back()->view == view) {
        previous_cut = global_cuts_.back().get();
    }
    GlobalCut* new_cut = new GlobalCut(view);
    new_cut->start_seqnum = record.start_seqnum();
    global_cuts_.emplace_back(new_cut);
    for (size_t i = 0; i < view->num_nodes(); i++) {
        new_cut->localid_cuts[i] = record.localid_cuts(i);
        uint16_t node_id = view->node(i);
        uint32_t start_localid = previous_cut == nullptr ? 0 : previous_cut->localid_cuts[i];
        uint32_t end_localid = new_cut->localid_cuts[i];
        if (start_localid < end_localid) {
            uint32_t delta = end_localid - start_localid;
            log_replicated_cb_(
                /* start_localid= */ BuildLocalId(view->id(), node_id, start_localid),
                /* start_seqnum= */  next_log_seqnum_,
                /* delta= */         delta
            );
            next_log_seqnum_ += delta;
            new_cut->deltas[i] = delta;
        } else if (start_localid > end_localid) {
            HLOG(FATAL) << "localid_cuts from GlobalCutRecordProto not increasing";
        }
    }
    new_cut->end_seqnum = next_log_seqnum_;
    if (new_cut->start_seqnum >= new_cut->end_seqnum) {
        HLOG(FATAL) << "New global cut does not replicate any new log";
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
    ComputeHashSeed();
}

Fsm::View::~View() {}

void Fsm::View::ForEachBackupNode(uint16_t primary_node_id,
                                  std::function<void(uint16_t /* node_id */)> cb) const {
    DCHECK(node_indices_.contains(primary_node_id));
    size_t base = node_indices_.at(primary_node_id);
    for (size_t i = 1; i < replicas_; i++) {
        size_t node_id = node_ids_[(base + i) % node_ids_.size()];
        cb(node_id);
    }
}

void Fsm::View::ForEachPrimaryNode(uint16_t backup_node_id,
                                   std::function<void(uint16_t /* node_id */)> cb) const {
    DCHECK(node_indices_.contains(backup_node_id));
    size_t base = node_indices_.at(backup_node_id);
    for (size_t i = 1; i < replicas_; i++) {
        size_t node_id = node_ids_[(base - i + node_ids_.size()) % node_ids_.size()];
        cb(node_id);
    }
}

uint16_t Fsm::View::PickOneNode() const {
    size_t idx = gsl::narrow_cast<size_t>(utils::GetRandomInt(0, node_ids_.size()));
    return node_ids_[idx];
}

bool Fsm::View::IsStorageNodeOf(uint16_t primary_node_id, uint16_t node_id) const {
    DCHECK(node_indices_.contains(primary_node_id));
    if (!node_indices_.contains(node_id)) {
        return false;
    }
    if (primary_node_id == node_id) {
        return true;
    }
    size_t base = node_indices_.at(primary_node_id);
    for (size_t i = 1; i < replicas_; i++) {
        if (node_id == node_ids_[(base + i) % node_ids_.size()]) {
            return true;
        }
    }
    return false;
}

uint16_t Fsm::View::PickOneStorageNode(uint16_t primary_node_id) const {
    DCHECK(node_indices_.contains(primary_node_id));
    size_t base = node_indices_.at(primary_node_id);
    size_t off = gsl::narrow_cast<size_t>(utils::GetRandomInt(0, replicas_));
    return node_ids_[(base + off) % node_ids_.size()];
}

uint16_t Fsm::View::LogTagToPrimaryNode(uint32_t log_tag) const {
    uint64_t h = hash::xxHash64(log_tag, hash_seed_);
    return node_ids_[h % node_ids_.size()];
}

void Fsm::View::ComputeHashSeed() {
    uint64_t v = (replicas_ << 16) + id_;
    for (size_t i = 0; i < node_ids_.size(); i++) {
        v = v * 1299833 + node_ids_[i];
    }
    hash_seed_ = hash::xxHash64(v);
}

}  // namespace log
}  // namespace faas
