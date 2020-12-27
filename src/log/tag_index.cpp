#include "log/tag_index.h"

#define log_header_ "TagIndex: "

namespace faas {
namespace log {

TagIndex::TagIndex()
    : storage_(new TagIndexStorage()),
      committed_seqnum_(0), applied_seqnum_(0),
      fsm_progress_(0) {}

TagIndex::~TagIndex() {}

void TagIndex::RecvTagData(uint16_t primary_node_id,  uint64_t start_seqnum,
                           const TagVec& tags) {
    if (pending_tag_data_.count(start_seqnum) == 0) {
        pending_tag_data_.insert({start_seqnum, std::make_pair(primary_node_id, tags)});
        Advance();
    } else {
        HLOG(WARNING) << "Received duplicate tag data";
    }
}

void TagIndex::OnNewView(uint32_t fsm_seqnum, uint16_t view_id) {
    DCHECK_EQ(size_t{fsm_seqnum}, fsm_progress_ + cuts_.size());
    if (fsm_seqnum == fsm_progress_) {
        DCHECK_EQ(committed_seqnum_, applied_seqnum_);
        applied_seqnum_ = BuildSeqNum(view_id, 0);
        committed_seqnum_ = applied_seqnum_;
        fsm_progress_++;
    } else {
        cuts_.push_back(BuildSeqNum(view_id, 0));
        Advance();
    }
}

void TagIndex::OnNewGlobalCut(uint32_t fsm_seqnum, uint64_t start_seqnum, uint64_t end_seqnum) {
    DCHECK_EQ(size_t{fsm_seqnum}, fsm_progress_ + cuts_.size());
    if (cuts_.empty()) {
        DCHECK_EQ(committed_seqnum_, start_seqnum);
    } else {
        DCHECK_EQ(cuts_.back(), start_seqnum);
    }
    cuts_.push_back(end_seqnum);
    Advance();
}

uint64_t TagIndex::learned_seqnum() const {
    if (!cuts_.empty()) {
        return cuts_.back();
    } else {
        DCHECK_EQ(committed_seqnum_, applied_seqnum_);
        return committed_seqnum_;
    }
}

void TagIndex::Advance() {
    auto iter = pending_tag_data_.begin();
    while (iter != pending_tag_data_.end()) {
        uint64_t start_seqnum = iter->first;
        DCHECK_GE(start_seqnum, applied_seqnum_);
        if (start_seqnum > applied_seqnum_ || start_seqnum >= learned_seqnum()) {
            break;
        }
        uint16_t primary_node_id = iter->second.first;
        const TagVec& tag_vec = iter->second.second;
        ApplyTagData(primary_node_id, start_seqnum, tag_vec);
        iter = pending_tag_data_.erase(iter);
        DCHECK(!cuts_.empty());
        while (!cuts_.empty()) {
            uint64_t cut = cuts_.front();
            DCHECK_GE(cut, applied_seqnum_);
            if (cut == applied_seqnum_) {
                committed_seqnum_ = cut;
                cuts_.pop_front();
                fsm_progress_++;
            } else if (SeqNumToViewId(cut) > SeqNumToViewId(committed_seqnum_)) {
                DCHECK_EQ(committed_seqnum_, applied_seqnum_);
                DCHECK_EQ(cut, BuildSeqNum(SeqNumToViewId(committed_seqnum_) + 1, 0));
                applied_seqnum_ = cut;
                committed_seqnum_ = cut;
                cuts_.pop_front();
                fsm_progress_++;
            } else {
                break;
            }
        }
    }
}

void TagIndex::ApplyTagData(uint16_t primary_node_id, uint64_t start_seqnum,
                            const TagVec& tags) {
    DCHECK_EQ(start_seqnum, applied_seqnum_);
    for (size_t i = 0; i < tags.size(); i++) {
        storage_->Add(tags.at(i), start_seqnum + i, primary_node_id);
    }
    applied_seqnum_ += tags.size();
}

IndexResult TagIndex::FindFirst(const IndexQuery& query) const {
    return storage_->FindFirst(query.tag, query.start_seqnum,
                               std::min(committed_seqnum_, query.end_seqnum));
}

IndexResult TagIndex::FindLast(const IndexQuery& query) const {
    return storage_->FindLast(query.tag, query.start_seqnum,
                              std::min(committed_seqnum_, query.end_seqnum));
}

void TagIndex::DoStateCheck(std::ostringstream& stream) const {
    stream << fmt::format("TagIndex: FsmProgress={} "
                          "ComittedSeqNum={:#018x} AppliedSeqNum={:#018x}\n",
                          fsm_progress_, committed_seqnum_, applied_seqnum_);
    if (!cuts_.empty()) {
        stream << "KnownCuts: [";
        for (size_t i = 0; i < cuts_.size(); i++) {
            if (i > 0) {
                stream << ", ";
            }
            stream << fmt::format("{:#018x}", cuts_.at(i));
        }
        stream << "]\n";
    }
}

constexpr IndexResult kEmptyIndexResult = {
    .seqnum = kInvalidLogSeqNum,
    .tag = kEmptyLogTag,
    .primary_node_id = 0
};

TagIndexStorage::TagIndexStorage() {}

TagIndexStorage::~TagIndexStorage() {}

void TagIndexStorage::Add(uint64_t tag, uint64_t seqnum, uint16_t primary_node_id) {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        per_tag_indices_[tag].reset(new PerTagIndex(tag));
    }
    per_tag_indices_[tag]->Add(seqnum, primary_node_id);
}

IndexResult TagIndexStorage::FindFirst(uint64_t tag, uint64_t start_seqnum,
                                       uint64_t end_seqnum) const {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        return kEmptyIndexResult;
    }
    return per_tag_indices_.at(tag)->FindFirst(start_seqnum, end_seqnum);
}

IndexResult TagIndexStorage::FindLast(uint64_t tag, uint64_t start_seqnum,
                                      uint64_t end_seqnum) const {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        return kEmptyIndexResult;
    }
    return per_tag_indices_.at(tag)->FindLast(start_seqnum, end_seqnum);
}

TagIndexStorage::PerTagIndex::PerTagIndex(uint64_t tag)
    : tag_(tag) {}

TagIndexStorage::PerTagIndex::~PerTagIndex() {}

void TagIndexStorage::PerTagIndex::Add(uint64_t seqnum, uint16_t primary_node_id) {
    DCHECK(indices_.empty() || indices_.back().seqnum < seqnum);
    indices_.push_back({.seqnum = seqnum, .node_id = primary_node_id});
}

IndexResult TagIndexStorage::PerTagIndex::FindFirst(uint64_t start_seqnum,
                                                    uint64_t end_seqnum) const {
    auto iter = absl::c_lower_bound(
        indices_, IndexElem { .seqnum = start_seqnum, .node_id = 0 });
    if (iter != indices_.end() && iter->seqnum < end_seqnum) {
        return {
            .seqnum = iter->seqnum,
            .tag = tag_,
            .primary_node_id = iter->node_id
        };
    } else {
        return kEmptyIndexResult;
    }
}

IndexResult TagIndexStorage::PerTagIndex::FindLast(uint64_t start_seqnum,
                                                   uint64_t end_seqnum) const {
    auto iter = absl::c_lower_bound(
        indices_, IndexElem{ .seqnum = end_seqnum, .node_id = 0 });
    if (iter != indices_.begin() && (--iter)->seqnum >= start_seqnum) {
        return {
            .seqnum = iter->seqnum,
            .tag = tag_,
            .primary_node_id = iter->node_id
        };
    } else {
        return kEmptyIndexResult;
    }
}

}  // namespace log
}  // namespace faas
