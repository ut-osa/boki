#include "log/sequencer.h"

#include "utils/bits.h"

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

Sequencer::Sequencer(uint16_t node_id)
    : SequencerBase(node_id),
      log_header_(fmt::format("Sequencer[{}]: ", node_id)),
      current_view_(nullptr) {}

Sequencer::~Sequencer() {}

void Sequencer::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    bool contains_myself = view->contains_sequencer_node(my_node_id());
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock core_lk(&core_mu_);
        if (contains_myself) {
            primary_collection_.InstallLogSpace(
                std::make_unique<MetaLogPrimary>(view, my_node_id()));
            for (uint16_t id : view->GetSequencerNodes()) {
                if (view->GetSequencerNode(id)->IsReplicaSequencerNode(my_node_id())) {
                    backup_collection_.InstallLogSpace(
                        std::make_unique<MetaLogBackup>(view, id));
                }
            }
        }
        current_primary_ = primary_collection_.GetLogSpace(
            bits::JoinTwo16(view->id(), my_node_id()));
        DCHECK(!contains_myself || current_primary_ != nullptr);
        {
            absl::MutexLock future_request_lk(&future_request_mu_);
            future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        }
        current_view_ = view;
    }
    if (!ready_requests.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] {
                ProcessRequests(requests);
            }
        );
    }
}

void Sequencer::OnViewFrozen(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
}

void Sequencer::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
}

#define ONHOLD_IF_FROM_FUTURE_VIEW(MESSAGE_VAR, PAYLOAD_VAR)        \
    do {                                                            \
        if (current_view_ == nullptr                                \
                || (MESSAGE_VAR).view_id > current_view_->id()) {   \
            absl::MutexLock future_request_lk(&future_request_mu_); \
            future_requests_.OnHoldRequest(                         \
                SharedLogRequest(MESSAGE_VAR, PAYLOAD_VAR));        \
            return;                                                 \
        }                                                           \
    } while (0)

#define PANIC_IF_FROM_FUTURE_VIEW(MESSAGE_VAR)                      \
    do {                                                            \
        if (current_view_ == nullptr                                \
                || (MESSAGE_VAR).view_id > current_view_->id()) {   \
            HLOG(FATAL) << "Receive message from future view "      \
                        << (MESSAGE_VAR).view_id;                   \
        }                                                           \
    } while (0)

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)                       \
    do {                                                            \
        if (current_view_ != nullptr                                \
                && (MESSAGE_VAR).view_id < current_view_->id()) {   \
            HLOG(WARNING) << "Receive outdate message from view "   \
                          << (MESSAGE_VAR).view_id;                 \
            return;                                                 \
        }                                                           \
    } while (0)

void Sequencer::HandleTrimRequest(const SharedLogMessage& request) {
    DCHECK(SharedLogMessageHelper::GetOpType(request) == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void Sequencer::OnRecvMetaLogProgress(const SharedLogMessage& message) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::META_PROG);
    LockablePtr<MetaLogPrimary> logspace_ptr;
    {
        absl::ReaderMutexLock core_lk(&core_mu_);
        PANIC_IF_FROM_FUTURE_VIEW(message);  // I believe this will never happen
        IGNORE_IF_FROM_PAST_VIEW(message);
        logspace_ptr = primary_collection_.GetLogSpaceChecked(message.logspace_id);
    }
    {
        auto locked_logspace = logspace_ptr.Lock();
        locked_logspace->UpdateReplicaProgress(
            message.origin_node_id, message.metalog_position);
    }
}

void Sequencer::OnRecvShardProgress(const SharedLogMessage& message,
                                    std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::SHARD_PROG);
    LockablePtr<MetaLogPrimary> logspace_ptr;
    {
        absl::ReaderMutexLock core_lk(&core_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        logspace_ptr = primary_collection_.GetLogSpaceChecked(message.logspace_id);
    }
    {
        auto locked_logspace = logspace_ptr.Lock();
        std::vector<uint32_t> progress(payload.size() / sizeof(uint32_t), 0);
        memcpy(progress.data(), payload.data(), payload.size());
        locked_logspace->UpdateStorageProgress(message.origin_node_id, progress);
    }
}

void Sequencer::OnRecvNewMetaLogs(const SharedLogMessage& message,
                                  std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), message.logspace_id);
    LockablePtr<MetaLogBackup> logspace_ptr;
    {
        absl::ReaderMutexLock core_lk(&core_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        logspace_ptr = backup_collection_.GetLogSpaceChecked(message.logspace_id);
    }
    uint32_t old_metalog_position;
    uint32_t new_metalog_position;
    {
        auto locked_logspace = logspace_ptr.Lock();
        old_metalog_position = locked_logspace->metalog_position();
        for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
            locked_logspace->ProvideMetaLog(metalog_proto);
        }
        new_metalog_position = locked_logspace->metalog_position();
    }
    if (new_metalog_position > old_metalog_position) {
        SharedLogMessage response = SharedLogMessageHelper::NewMetaLogProgressMessage(
            message.logspace_id, new_metalog_position);
        SendSequencerMessage(message.sequencer_id, &response);
    }
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef PANIC_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

void Sequencer::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_TO_SPAN(request.payload));
    }
}

void Sequencer::MarkNextCutIfDoable() {

}

}  // namespace log
}  // namespace faas
