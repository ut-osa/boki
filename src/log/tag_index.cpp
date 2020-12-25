#include "log/tag_index.h"

namespace faas {
namespace log {

TagIndex::TagIndex() {}

TagIndex::~TagIndex() {}

void TagIndex::Add(uint64_t tag, uint64_t seqnum) {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        per_tag_indices_[tag].reset(new PerTagIndex(tag));
    }
    per_tag_indices_[tag]->Add(seqnum);
}

uint64_t TagIndex::ReadFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        return kInvalidLogSeqNum;
    }
    return per_tag_indices_.at(tag)->ReadFirst(start_seqnum, end_seqnum);
}

uint64_t TagIndex::ReadLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const {
    DCHECK(tag != kEmptyLogTag);
    if (!per_tag_indices_.contains(tag)) {
        return kInvalidLogSeqNum;
    }
    return per_tag_indices_.at(tag)->ReadLast(start_seqnum, end_seqnum);
}

TagIndex::PerTagIndex::PerTagIndex(uint64_t tag)
    : tag_(tag) {}

TagIndex::PerTagIndex::~PerTagIndex() {}

void TagIndex::PerTagIndex::Add(uint64_t seqnum) {
    indices_.insert(seqnum);
}

uint64_t TagIndex::PerTagIndex::ReadFirst(uint64_t start_seqnum, uint64_t end_seqnum) const {
    auto iter = indices_.lower_bound(start_seqnum);
    if (iter != indices_.end() && *iter < end_seqnum) {
        return *iter;
    } else {
        return kInvalidLogSeqNum;
    }
}

uint64_t TagIndex::PerTagIndex::ReadLast(uint64_t start_seqnum, uint64_t end_seqnum) const {
    auto iter = indices_.lower_bound(end_seqnum);
    if (iter != indices_.begin() && *(--iter) >= start_seqnum) {
        return *iter;
    } else {
        return kInvalidLogSeqNum;
    }
}

}  // namespace log
}  // namespace faas
