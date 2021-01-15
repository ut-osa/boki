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

struct SharedLogRequest {
    protocol::SharedLogMessage message;
    std::string                payload;
    void*                      local_op;

    explicit SharedLogRequest(const protocol::SharedLogMessage& message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN)
        : message(message),
          payload(),
          local_op(nullptr) {
        if (payload.size() > 0) {
            this->payload.assign(payload.data(), payload.size());
        }
    }
};

struct LogMetaData {
    uint32_t user_logspace;
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
