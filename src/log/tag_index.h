#pragma once

#include "log/common.h"

namespace faas {
namespace log {

class TagIndexStorage;

class TagIndex {
public:
    TagIndex();
    ~TagIndex();

    uint32_t fsm_progress() const { return fsm_progress_; }

    // Tag data might be received out-of-order
    typedef absl::FixedArray<uint64_t> TagVec;
    void RecvTagData(uint64_t start_seqnum, const TagVec& tags);

    void OnNewView(uint32_t fsm_seqnum, uint16_t view_id);
    void OnNewGlobalCut(uint32_t fsm_seqnum, uint64_t start_seqnum, uint64_t end_seqnum);

    uint64_t FindFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const;
    uint64_t FindLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const;

private:
    std::unique_ptr<TagIndexStorage> storage_;

    uint64_t committed_seqnum_;
    uint64_t applied_seqnum_;

    std::map</* start_seqnum */ uint64_t, TagVec> pending_tag_data_;

    uint32_t fsm_progress_;
    std::queue</* end_seqnum */ uint64_t> cuts_;

    uint64_t learned_seqnum() const;
    void Advance();
    void ApplyTagData(uint64_t start_seqnum, const TagVec& tags);

    DISALLOW_COPY_AND_ASSIGN(TagIndex);
};

class TagIndexStorage {
public:
    TagIndexStorage();
    ~TagIndexStorage();

    void Add(uint64_t tag, uint64_t seqnum);
    uint64_t FindFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const;
    uint64_t FindLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const;

private:
    class PerTagIndex {
    public:
        explicit PerTagIndex(uint64_t tag);
        ~PerTagIndex();
        void Add(uint64_t seqnum);
        uint64_t FindFirst(uint64_t start_seqnum, uint64_t end_seqnum) const;
        uint64_t FindLast(uint64_t start_seqnum, uint64_t end_seqnum) const;
    private:
        uint64_t tag_;
        std::vector</* seqnum */ uint64_t> indices_;
        DISALLOW_COPY_AND_ASSIGN(PerTagIndex);
    };

    absl::flat_hash_map</* tag */ uint64_t,
                        std::unique_ptr<PerTagIndex>> per_tag_indices_;

    DISALLOW_COPY_AND_ASSIGN(TagIndexStorage);
};

}  // namespace log
}  // namespace faas
