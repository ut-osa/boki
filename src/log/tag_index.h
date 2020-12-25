#pragma once

#include "log/common.h"

namespace faas {
namespace log {

class TagIndex {
public:
    TagIndex();
    ~TagIndex();

    void Add(uint64_t tag, uint64_t seqnum);
    uint64_t ReadFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const;
    uint64_t ReadLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum) const;

private:
    class PerTagIndex {
    public:
        explicit PerTagIndex(uint64_t tag);
        ~PerTagIndex();
        void Add(uint64_t seqnum);
        uint64_t ReadFirst(uint64_t start_seqnum, uint64_t end_seqnum) const;
        uint64_t ReadLast(uint64_t start_seqnum, uint64_t end_seqnum) const;
    private:
        uint64_t tag_;
        std::set</* seqnum */ uint64_t> indices_;
        DISALLOW_COPY_AND_ASSIGN(PerTagIndex);
    };

    absl::flat_hash_map</* tag */ uint64_t,
                        std::unique_ptr<PerTagIndex>> per_tag_indices_;

    DISALLOW_COPY_AND_ASSIGN(TagIndex);
};

}  // namespace log
}  // namespace faas
