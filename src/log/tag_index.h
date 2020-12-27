#pragma once

#include "log/common.h"

namespace faas {
namespace log {

class TagIndexStorage;

struct IndexQuery {
    uint64_t tag;
    uint64_t start_seqnum;
    uint64_t end_seqnum;
};

struct IndexResult {
    uint64_t seqnum;
    uint64_t tag;
    uint16_t primary_node_id;
};

class TagIndex {
public:
    TagIndex();
    ~TagIndex();

    uint32_t fsm_progress() const { return fsm_progress_; }

    // Tag data could be received out-of-order
    typedef absl::FixedArray<uint64_t> TagVec;
    void RecvTagData(uint16_t primary_node_id, uint64_t start_seqnum, const TagVec& tags);

    void OnNewView(uint32_t fsm_seqnum, uint16_t view_id);
    void OnNewGlobalCut(uint32_t fsm_seqnum, uint64_t start_seqnum, uint64_t end_seqnum);

    IndexResult FindFirst(const IndexQuery& query) const;
    IndexResult FindLast(const IndexQuery& query) const;

    void DoStateCheck(std::ostringstream& stream) const;

private:
    std::unique_ptr<TagIndexStorage> storage_;

    uint64_t committed_seqnum_;
    uint64_t applied_seqnum_;

    std::map</* start_seqnum */ uint64_t, std::pair<uint16_t, TagVec>> pending_tag_data_;

    uint32_t fsm_progress_;
    std::deque</* end_seqnum */ uint64_t> cuts_;

    uint64_t learned_seqnum() const;
    void Advance();
    void ApplyTagData(uint16_t primary_node_id, uint64_t start_seqnum, const TagVec& tags);

    DISALLOW_COPY_AND_ASSIGN(TagIndex);
};

class TagIndexStorage {
public:
    TagIndexStorage();
    ~TagIndexStorage();

    void Add(uint64_t tag, uint64_t seqnum, uint16_t primary_node_id);
    IndexResult FindFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const;
    IndexResult FindLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const;

private:
    class PerTagIndex {
    public:
        explicit PerTagIndex(uint64_t tag);
        ~PerTagIndex();
        void Add(uint64_t seqnum, uint16_t primary_node_id);
        IndexResult FindFirst(uint64_t start_seqnum, uint64_t end_seqnum) const;
        IndexResult FindLast(uint64_t start_seqnum, uint64_t end_seqnum) const;
    private:
        uint64_t tag_;

        struct IndexElem {
            uint64_t seqnum;
            uint16_t node_id;
            bool operator<(const IndexElem& other) const {
                return seqnum < other.seqnum;
            }
        };
        std::vector<IndexElem> indices_;
        
        DISALLOW_COPY_AND_ASSIGN(PerTagIndex);
    };

    absl::flat_hash_map</* tag */ uint64_t,
                        std::unique_ptr<PerTagIndex>> per_tag_indices_;

    DISALLOW_COPY_AND_ASSIGN(TagIndexStorage);
};

}  // namespace log
}  // namespace faas
