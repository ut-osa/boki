#include "log/utils.h"

#include "utils/bits.h"

namespace faas {
namespace log_utils {

using log::View;
using log::SharedLogRequest;
using log::LogMetaData;
using log::LogEntryProto;
using log::MetaLogProto;
using log::MetaLogsProto;
using protocol::SharedLogMessage;

uint16_t GetViewId(uint64_t value) {
    return bits::HighHalf32(bits::HighHalf64(value));
}

FutureRequests::FutureRequests()
    : next_view_id_(0) {}

FutureRequests::~FutureRequests() {}

void FutureRequests::OnNewView(const View* view,
                               std::vector<SharedLogRequest>* ready_requests) {
    absl::MutexLock lk(&mu_);
    if (view->id() != next_view_id_) {
        LOG_F(FATAL, "Views are not consecutive: have={}, expect={}",
              view->id(), next_view_id_);
    }
    if (onhold_requests_.contains(view->id())) {
        if (ready_requests == nullptr) {
            LOG_F(FATAL, "Not expect on-hold requests for view {}", view->id());
        }
        *ready_requests = std::move(onhold_requests_[view->id()]);
        onhold_requests_.erase(view->id());
    }
    next_view_id_++;
}

void FutureRequests::OnHoldRequest(uint16_t view_id, SharedLogRequest request) {
    absl::MutexLock lk(&mu_);
    if (view_id < next_view_id_) {
        LOG_F(FATAL, "Receive request from view not in the future: "
                      "request_view_id={}, next_view_id={}",
              view_id, next_view_id_);
    }
    onhold_requests_[view_id].push_back(std::move(request));
}

MetaLogsProto MetaLogsFromPayload(std::span<const char> payload) {
    MetaLogsProto metalogs_proto;
    if (!metalogs_proto.ParseFromArray(payload.data(),
                                       static_cast<int>(payload.size()))) {
        LOG(FATAL) << "Failed to parse MetaLogsProto";
    }
    if (metalogs_proto.metalogs_size() == 0) {
        LOG(FATAL) << "Empty MetaLogsProto";
    }
    uint32_t logspace_id = metalogs_proto.logspace_id();
    for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
        if (metalog_proto.logspace_id() != logspace_id) {
            LOG(FATAL) << "Meta logs in on MetaLogsProto must have the same logspace_id";
        }
    }
    return metalogs_proto;
}

LogMetaData GetMetaDataFromMessage(const SharedLogMessage& message) {
    size_t total_size = message.payload_size;
    size_t num_tags = message.num_tags;
    size_t aux_data_size = message.aux_data_size;
    DCHECK_LT(num_tags * sizeof(uint64_t) + aux_data_size, total_size);
    size_t log_data_size = total_size - num_tags * sizeof(uint64_t) - aux_data_size;
    return LogMetaData {
        .user_logspace = message.user_logspace,
        .seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf),
        .localid = message.localid,
        .num_tags = num_tags,
        .data_size = log_data_size
    };
}

void SplitPayloadForMessage(const protocol::SharedLogMessage& message,
                            std::span<const char> payload,
                            std::span<const uint64_t>* user_tags,
                            std::span<const char>* log_data,
                            std::span<const char>* aux_data) {
    size_t total_size = message.payload_size;
    DCHECK_EQ(payload.size(), total_size);
    size_t num_tags = message.num_tags;
    size_t aux_data_size = message.aux_data_size;
    DCHECK_LT(num_tags * sizeof(uint64_t) + aux_data_size, total_size);
    size_t log_data_size = total_size - num_tags * sizeof(uint64_t) - aux_data_size;
    const char* ptr = payload.data();
    if (user_tags != nullptr) {
        if (num_tags > 0) {
            *user_tags = std::span<const uint64_t>(
                reinterpret_cast<const uint64_t*>(ptr), num_tags);
        } else {
            *user_tags = std::span<const uint64_t>();
        }
    }
    ptr += num_tags * sizeof(uint64_t);
    if (log_data != nullptr) {
        *log_data = std::span<const char>(ptr, log_data_size);
    }
    ptr += log_data_size;
    if (aux_data != nullptr) {
        if (aux_data_size > 0) {
            *aux_data = std::span<const char>(ptr, aux_data_size);
        } else {
            *aux_data = std::span<const char>();
        }
    }
}

void SplitLogEntryProto(const log::LogEntryProto& log_entry_proto,
                        log::LogMetaData* metadata,
                        std::span<const uint64_t>* user_tags,
                        std::span<const char>* log_data) {
    metadata->user_logspace = log_entry_proto.user_logspace();
    metadata->seqnum = log_entry_proto.seqnum();
    metadata->localid = log_entry_proto.localid();
    metadata->num_tags = static_cast<size_t>(log_entry_proto.user_tags_size());
    metadata->data_size = log_entry_proto.data().size();
    *user_tags = std::span<const uint64_t>(
        reinterpret_cast<const uint64_t*>(log_entry_proto.user_tags().data()),
        static_cast<size_t>(log_entry_proto.user_tags().size()));
    *log_data = STRING_AS_SPAN(log_entry_proto.data());
}

void PopulateMetaDataToMessage(const LogMetaData& metadata, SharedLogMessage* message) {
    message->logspace_id = bits::HighHalf64(metadata.seqnum);
    message->user_logspace = metadata.user_logspace;
    message->seqnum_lowhalf = bits::LowHalf64(metadata.seqnum);
    message->num_tags = gsl::narrow_cast<uint16_t>(metadata.num_tags);
    message->localid = metadata.localid;
}

// void PopulateMetaDataToMessage(const LogEntryProto& log_entry, SharedLogMessage* message) {
//     message->logspace_id = bits::HighHalf64(log_entry.seqnum());
//     message->user_logspace = log_entry.user_logspace();
//     message->seqnum_lowhalf = bits::LowHalf64(log_entry.seqnum());
//     message->num_tags = gsl::narrow_cast<uint16_t>(log_entry.user_tags_size());
//     message->localid = log_entry.localid();
// }

}  // namespace log_utils
}  // namespace faas
