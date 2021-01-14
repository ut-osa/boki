#include "log/utils.h"

namespace faas {
namespace log_utils {

using log::View;
using log::SharedLogRequest;
using log::LogMetaData;
using log::LogEntryProto;
using log::MetaLogProto;
using log::MetaLogsProto;
using protocol::SharedLogMessage;

FutureRequests::FutureRequests() {}

FutureRequests::~FutureRequests() {}

void FutureRequests::OnNewView(const View* view,
                               std::vector<SharedLogRequest>* ready_requests) {
    if (view->id() != next_view_id_) {
        LOG(FATAL) << fmt::format("Views are not consecutive: have={}, expect={}",
                                  view->id(), next_view_id_);
    }
    
    if (onhold_requests_.contains(view->id())) {
        if (ready_requests == nullptr) {
            LOG(FATAL) << fmt::format("Not expect on-hold requests for view {}", view->id());
        }
        *ready_requests = std::move(onhold_requests_[view->id()]);
        onhold_requests_.erase(view->id());
    }
    next_view_id_++;
}

void FutureRequests::OnHoldRequest(SharedLogRequest request) {
    uint16_t view_id = request.message.view_id;
    if (view_id < next_view_id_) {
        LOG(FATAL) << fmt::format("Receive request from view not in the future: "
                                  "request_view_id={}, next_view_id={}",
                                  view_id, next_view_id_);
    }
    onhold_requests_[view_id].push_back(std::move(request));
}

MetaLogsProto MetaLogsFromPayload(std::span<const char> payload) {
    MetaLogsProto metalogs_proto;
    if (!metalogs_proto.ParseFromArray(payload.data(), payload.size())) {
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
    return LogMetaData {
        .user_logspace = message.user_logspace,
        .logspace_id = message.logspace_id,
        .user_tag = message.user_tag,
        .seqnum = message.seqnum,
        .localid = message.localid
    };
}

void PopulateMetaDataToMessage(const LogMetaData& metadata, SharedLogMessage* message) {
    message->logspace_id = metadata.logspace_id;
    message->user_logspace = metadata.user_logspace;
    message->user_tag = metadata.user_tag;
    message->seqnum = metadata.seqnum;
    message->localid = metadata.localid;
}

void PopulateMetaDataToMessage(const LogEntryProto& log_entry, SharedLogMessage* message) {
    message->logspace_id = log_entry.logspace_id();
    message->user_logspace = log_entry.user_logspace();
    message->user_tag = log_entry.user_tag();
    message->seqnum = log_entry.seqnum();
    message->localid = log_entry.localid();
}

}  // namespace log_utils
}  // namespace faas
