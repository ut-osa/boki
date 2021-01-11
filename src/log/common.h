#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "proto/shared_log.pb.h"
#include "utils/appendable_buffer.h"

namespace faas {
namespace log {

constexpr uint64_t kEmptyLogTag      = 0;
constexpr uint64_t kMaxLogSeqNum     = 0xffff000000000000ULL;
constexpr uint64_t kInvalidLogSeqNum = protocol::kInvalidLogSeqNum;

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

inline std::string SeqNumHexStr(uint64_t seqnum) {
    return fmt::format("{:016x}", seqnum);
}

struct LogRecord {
    uint64_t localid;
    uint64_t seqnum;
    uint64_t tag;
    std::string data;
};

struct SharedLogRequest {
    protocol::SharedLogMessage message;
    std::string                payload;

    explicit SharedLogRequest(const protocol::SharedLogMessage& message,
                              std::span<const char> payload = std::span<const char>())
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

inline void PopulateMetaDataFromRequest(const protocol::SharedLogMessage& request,
                                        LogMetaData* metadata) {
    metadata->logspace_id = request.logspace_id;
    metadata->user_logspace = request.user_logspace;
    metadata->user_tag = request.user_tag;
    metadata->seqnum = request.seqnum;
    metadata->localid = request.localid;
}

inline void PopulateMetaDataToResponse(const LogMetaData& metadata,
                                       protocol::SharedLogMessage* response) {
    response->logspace_id = metadata.logspace_id;
    response->user_logspace = metadata.user_logspace;
    response->user_tag = metadata.user_tag;
    response->seqnum = metadata.seqnum;
    response->localid = metadata.localid;
}

inline void PopulateMetaDataToResponse(const LogEntryProto& log_entry,
                                       protocol::SharedLogMessage* response) {
    response->logspace_id = log_entry.logspace_id();
    response->user_logspace = log_entry.user_logspace();
    response->user_tag = log_entry.user_tag();
    response->seqnum = log_entry.seqnum();
    response->localid = log_entry.localid();
}

}  // namespace log
}  // namespace faas
