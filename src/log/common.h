#pragma once

#include "base/common.h"
#include "proto/shared_log.pb.h"

namespace faas {
namespace log {

inline uint16_t SeqNumToViewId(uint64_t seqnum) {
    return gsl::narrow_cast<uint16_t>(seqnum >> 48);
}

inline uint16_t LocalIdToViewId(uint64_t localid) {
    return gsl::narrow_cast<uint16_t>(localid >> 48);
}

inline uint16_t LocalIdToNodeId(uint64_t localid) {
    return gsl::narrow_cast<uint16_t>((localid >> 32) & 0xffff);
}

inline uint32_t LocalIdToCounter(uint64_t localid) {
    return gsl::narrow_cast<uint32_t>(localid & 0xffffffff);
}

inline uint64_t BuildSeqNum(uint16_t view_id, uint64_t lower) {
    return (uint64_t{view_id} << 48) + lower;
}

inline uint64_t BuildLocalId(uint16_t view_id, uint16_t node_id, uint32_t counter) {
    return (uint64_t{view_id} << 48) + (uint64_t{node_id} << 32) + uint64_t{counter};
}

struct LogEntry {
    uint64_t localid;
    uint64_t seqnum;
    uint32_t tag;
    std::string data;
};

}  // namespace log
}  // namespace faas
