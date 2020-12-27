#include "log/tag_index.h"

#define log_header_ "TagIndex: "

namespace faas {
namespace log {

TagIndex::TagIndex()
    : storage_(new TagIndexStorage()),
      committed_seqnum_(0), applied_seqnum_(0),
      fsm_progress_(0) {}

TagIndex::~TagIndex() {}

void TagIndex::RecvTagData(uint64_t start_seqnum, const TagVec& tags) {
    if (pending_tag_data_.count(start_seqnum) == 0) {
        pending_tag_data_.insert({start_seqnum, tags});
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
        cuts_.push(BuildSeqNum(view_id, 0));
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
    cuts_.push(end_seqnum);
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
        ApplyTagData(start_seqnum, iter->second);
        iter = pending_tag_data_.erase(iter);
        DCHECK(!cuts_.empty());
        while (!cuts_.empty()) {
            uint64_t cut = cuts_.front();
            DCHECK_GE(cut, applied_seqnum_);
            if (cut == applied_seqnum_) {
                committed_seqnum_ = cut;
                cuts_.pop();
                fsm_progress_++;
            } else if (SeqNumToViewId(cut) > SeqNumToViewId(committed_seqnum_)) {
                DCHECK_EQ(committed_seqnum_, applied_seqnum_);
                DCHECK_EQ(cut, BuildSeqNum(SeqNumToViewId(committed_seqnum_) + 1, 0));
                applied_seqnum_ = cut;
                committed_seqnum_ = cut;
                cuts_.pop();
                fsm_progress_++;
            } else {
                break;
            }
        }
    }
}

void TagIndex::ApplyTagData(uint64_t start_seqnum, const TagVec& tags) {
    DCHECK_EQ(start_seqnum, applied_seqnum_);
    for (size_t i = 0; i < tags.size(); i++) {
        storage_->Add(tags.at(i), start_seqnum + i);
    }
    applied_seqnum_ += tags.size();
}

uint64_t TagIndex::FindFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const {
    return storage_->FindFirst(tag, start_seqnum, std::min(committed_seqnum_, end_seqnum));
}

uint64_t TagIndex::FindLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const {
    return storage_->FindFirst(tag, start_seqnum, std::min(committed_seqnum_, end_seqnum));
}

TagIndexStorage::TagIndexStorage() {}

TagIndexStorage::~TagIndexStorage() {}

void TagIndexStorage::Add(uint64_t tag, uint64_t seqnum) {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        per_tag_indices_[tag].reset(new PerTagIndex(tag));
    }
    per_tag_indices_[tag]->Add(seqnum);
}

uint64_t TagIndexStorage::FindFirst(uint64_t tag,
                                    uint64_t start_seqnum, uint64_t end_seqnum) const {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        return kInvalidLogSeqNum;
    }
    return per_tag_indices_.at(tag)->FindFirst(start_seqnum, end_seqnum);
}

uint64_t TagIndexStorage::FindLast(uint64_t tag,
                                   uint64_t start_seqnum, uint64_t end_seqnum) const {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        return kInvalidLogSeqNum;
    }
    return per_tag_indices_.at(tag)->FindLast(start_seqnum, end_seqnum);
}

TagIndexStorage::PerTagIndex::PerTagIndex(uint64_t tag)
    : tag_(tag) {}

TagIndexStorage::PerTagIndex::~PerTagIndex() {}

void TagIndexStorage::PerTagIndex::Add(uint64_t seqnum) {
    DCHECK(indices_.empty() || indices_.back() < seqnum);
    indices_.push_back(seqnum);
}

uint64_t TagIndexStorage::PerTagIndex::FindFirst(uint64_t start_seqnum,
                                                 uint64_t end_seqnum) const {
    auto iter = std::lower_bound(indices_.begin(), indices_.end(), start_seqnum);
    if (iter != indices_.end() && *iter < end_seqnum) {
        return *iter;
    } else {
        return kInvalidLogSeqNum;
    }
}

uint64_t TagIndexStorage::PerTagIndex::FindLast(uint64_t start_seqnum,
                                                uint64_t end_seqnum) const {
    auto iter = std::lower_bound(indices_.begin(), indices_.end(), end_seqnum);
    if (iter != indices_.begin() && *(--iter) >= start_seqnum) {
        return *iter;
    } else {
        return kInvalidLogSeqNum;
    }
}

}  // namespace log
}  // namespace faas
