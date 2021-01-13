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

void PopulateMetaDataFromRequest(const SharedLogMessage& request, LogMetaData* metadata) {
    metadata->logspace_id = request.logspace_id;
    metadata->user_logspace = request.user_logspace;
    metadata->user_tag = request.user_tag;
    metadata->seqnum = request.seqnum;
    metadata->localid = request.localid;
}

void PopulateMetaDataToResponse(const LogMetaData& metadata, SharedLogMessage* response) {
    response->logspace_id = metadata.logspace_id;
    response->user_logspace = metadata.user_logspace;
    response->user_tag = metadata.user_tag;
    response->seqnum = metadata.seqnum;
    response->localid = metadata.localid;
}

void PopulateMetaDataToResponse(const LogEntryProto& log_entry, SharedLogMessage* response) {
    response->logspace_id = log_entry.logspace_id();
    response->user_logspace = log_entry.user_logspace();
    response->user_tag = log_entry.user_tag();
    response->seqnum = log_entry.seqnum();
    response->localid = log_entry.localid();
}

}  // namespace log_utils
}  // namespace faas
