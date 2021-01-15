#include "log/engine.h"

#include "engine/engine.h"
#include "utils/bits.h"

namespace faas {
namespace log {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

Engine::Engine(engine::Engine* engine)
    : EngineBase(engine),
      log_header_(fmt::format("LogEngine[{}-N]: ", my_node_id())),
      current_view_(nullptr),
      current_view_active_(false) {}

Engine::~Engine() {}

void Engine::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    bool contains_myself = view->contains_engine_node(my_node_id());
    if (!contains_myself) {
        HLOG(WARNING) << fmt::format("View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    std::vector<std::pair<LogMetaData, LocalOp*>> new_appends;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                producer_collection_.InstallLogSpace(std::make_unique<LogProducer>(
                    my_node_id(), view, sequencer_id));
            }
        }
        {
            absl::MutexLock future_request_lk(&future_request_mu_);
            future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        }
        current_view_ = view;
        if (contains_myself) {
            current_view_active_ = true;
            absl::MutexLock pending_appends_lk(&pending_appends_mu_);
            if (!pending_appends_.empty()) {
                for (const auto& [op_id, op] : pending_appends_) {
                    LogMetaData log_metadata = MetaDataFromAppendOp(op);
                    auto producer_ptr = producer_collection_.GetLogSpaceChecked(
                        view->LogSpaceIdentifier(op->user_logspace));
                    producer_ptr.Lock()->LocalAppend(op, &log_metadata.localid);
                    new_appends.push_back(std::make_pair(log_metadata, op));
                }
                pending_appends_.clear();
            }
        }
        views_.push_back(view);
        log_header_ = fmt::format("LogEngine[{}-{}]: ", my_node_id(), view->id());
    }
    if (!ready_requests.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] () {
                ProcessRequests(requests);
            }
        );
    }
    if (!new_appends.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, view, new_appends = std::move(new_appends)] () {
                for (const auto& [log_metadata, op] : new_appends) {
                    ReplicateLogEntry(view, log_metadata, op->data.to_span());
                }
            }
        );
    }
}

void Engine::OnViewFrozen(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    absl::MutexLock view_lk(&view_mu_);
    DCHECK_EQ(view->id(), current_view_->id());
    if (view->contains_engine_node(my_node_id())) {
        DCHECK(current_view_active_);
        current_view_active_ = false;
    }
}

void Engine::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    LogProducer::AppendResultVec results;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        producer_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &results, this] (uint32_t logspace_id,
                                              LockablePtr<LogProducer> producer_ptr) {
                auto locked_producer = producer_ptr.Lock();
                bool success = locked_producer->Finalize(
                    finalized_view->final_metalog_position(logspace_id),
                    finalized_view->tail_metalogs(logspace_id));
                if (!success) {
                    HLOG(FATAL) << fmt::format("Failed to finalize log space {}",
                                                bits::HexStr0x(logspace_id));
                }
                LogProducer::AppendResultVec tmp;
                locked_producer->PollAppendResults(&tmp);
                results.insert(results.end(), tmp.begin(), tmp.end());
            }
        );
        if (!results.empty()) {
            absl::MutexLock pending_appends_lk(&pending_appends_mu_);
            LogProducer::AppendResultVec tmp;
            for (const LogProducer::AppendResult& result : results) {
                if (result.seqnum == kInvalidLogSeqNum) {
                    LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
                    DCHECK(pending_appends_.count(op->id) == 0);
                    pending_appends_[op->id] = op;
                } else {
                    tmp.push_back(result);
                }
            }
            results = std::move(tmp);
        }
    }
    ProcessAppendResults(results);
}

// Start handlers for local requests (from functions)

#define ONHOLD_IF_FROM_FUTURE_VIEW(LOCAL_OP_VAR)                        \
    do {                                                                \
        if (current_view_ == nullptr                                    \
                || GetLastViewId(LOCAL_OP_VAR) > current_view_->id()) { \
            absl::MutexLock future_request_lk(&future_request_mu_);     \
            SharedLogRequest local_request;                             \
            local_request.local_op = (LOCAL_OP_VAR);                    \
            future_requests_.OnHoldRequest(std::move(local_request));   \
            return;                                                     \
        }                                                               \
    } while (0)

void Engine::HandleLocalAppend(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::APPEND);
    const View* view = nullptr;
    LogMetaData log_metadata = MetaDataFromAppendOp(op);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (!current_view_active_) {
            absl::MutexLock pending_appends_lk(&pending_appends_mu_);
            DCHECK(pending_appends_.count(op->id) == 0);
            pending_appends_[op->id] = op;
            return;
        }
        view = current_view_;
        uint32_t logspace_id = view->LogSpaceIdentifier(op->user_logspace);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            locked_producer->LocalAppend(op, &log_metadata.localid);
        }
    }
    ReplicateLogEntry(view, log_metadata, op->data.to_span());
}

void Engine::HandleLocalTrim(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void Engine::HandleLocalRead(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::READ_NEXT
             || op->type == SharedLogOpType::READ_PREV);
    NOT_IMPLEMENTED();
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW

// Start handlers for remote messages

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

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)                       \
    do {                                                            \
        if (current_view_ != nullptr                                \
                && (MESSAGE_VAR).view_id < current_view_->id()) {   \
            HLOG(WARNING) << "Receive outdate request from view "   \
                          << (MESSAGE_VAR).view_id;                 \
            return;                                                 \
        }                                                           \
    } while (0)

void Engine::HandleRemoteRead(const SharedLogMessage& request) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(op_type == SharedLogOpType::READ_NEXT
             || op_type == SharedLogOpType::READ_PREV);
    NOT_IMPLEMENTED();
}

void Engine::OnRecvNewMetaLogs(const SharedLogMessage& message,
                               std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), message.logspace_id);
    LogProducer::AppendResultVec results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                locked_producer->ProvideMetaLog(metalog_proto);
            }
            locked_producer->PollAppendResults(&results);
        }
    }
    ProcessAppendResults(results);
}

void Engine::OnRecvNewIndexData(const SharedLogMessage& message,
                                std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_DATA);
    NOT_IMPLEMENTED();
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

void Engine::OnRecvResponse(const SharedLogMessage& message,
                            std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::RESPONSE);
}

void Engine::ProcessAppendResults(const LogProducer::AppendResultVec& results) {
    for (const LogProducer::AppendResult& result : results) {
        DCHECK_NE(result.seqnum, kInvalidLogSeqNum);
        LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
        Message response = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogResultType::APPEND_OK, result.seqnum);
        FinishLocalOpWithResponse(op, &response, result.metalog_progress);
    }
}

void Engine::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        if (request.local_op == nullptr) {
            MessageHandler(request.message, STRING_TO_SPAN(request.payload));
        } else {
            LocalOpHandler(reinterpret_cast<LocalOp*>(request.local_op));
        }
    }
}

}  // namespace log
}  // namespace faas
