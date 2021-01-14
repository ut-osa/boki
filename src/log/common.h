#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "proto/shared_log.pb.h"

namespace faas {
namespace log {

constexpr uint64_t kEmptyLogTag      = 0;
constexpr uint64_t kMaxLogSeqNum     = 0xffff000000000000ULL;
constexpr uint64_t kInvalidLogSeqNum = protocol::kInvalidLogSeqNum;
constexpr uint64_t kInvalidLogTag    = protocol::kInvalidLogTag;

inline uint16_t SeqNumToViewId(uint64_t seqnum) {
    return gsl::narrow_cast<uint16_t>(seqnum >> 48);
}

inline uint64_t BuildSeqNum(uint16_t view_id, uint64_t lower) {
    return (uint64_t{view_id} << 48) + lower;
}

struct SharedLogRequest {
    protocol::SharedLogMessage message;
    std::string                payload;

    explicit SharedLogRequest(const protocol::SharedLogMessage& message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN)
        : message(message),
          payload() {
        if (payload.size() > 0) {
            this->payload.assign(payload.data(), payload.size());
        }
    }
};

struct LogMetaData {
    uint32_t user_logspace;
    uint32_t logspace_id;
    uint64_t user_tag;
    uint64_t seqnum;
    uint64_t localid;
};

struct LogEntry {
    LogMetaData metadata;
    std::string data;
};

}  // namespace log
}  // namespace faas
