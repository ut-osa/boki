#include "log/engine_base.h"

#include "common/time.h"
#include "log/flags.h"
#include "log/utils.h"
#include "server/constants.h"
#include "engine/engine.h"
#include "utils/bits.h"

#define log_header_ "LogEngineBase: "

namespace faas {
namespace log {

using protocol::FuncCall;
using protocol::FuncCallHelper;
using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

EngineBase::EngineBase(engine::Engine* engine)
    : node_id_(engine->node_id_),
      engine_(engine),
      next_local_op_id_(0) {}

EngineBase::~EngineBase() {}

zk::ZKSession* EngineBase::zk_session() {
    return engine_->zk_session();
}

void EngineBase::Start() {
    SetupZKWatchers();
    SetupTimers();
    // Setup cache
    if (absl::GetFlag(FLAGS_slog_engine_enable_cache)) {
        log_cache_.reset(
            new LRUCache(absl::GetFlag(FLAGS_slog_engine_cache_cap_mb)));
    }
}

void EngineBase::Stop() {}

void EngineBase::SetupZKWatchers() {
    view_watcher_.SetViewCreatedCallback(
        [this] (const View* view) {
            this->OnViewCreated(view);
        }
    );
    view_watcher_.SetViewFrozenCallback(
        [this] (const View* view) {
            this->OnViewFrozen(view);
        }
    );
    view_watcher_.SetViewFinalizedCallback(
        [this] (const FinalizedView* finalized_view) {
            this->OnViewFinalized(finalized_view);
        }
    );
    view_watcher_.StartWatching(zk_session());
}

void EngineBase::SetupTimers() {
}

void EngineBase::OnNewExternalFuncCall(const FuncCall& func_call, uint32_t log_space) {
    absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
    if (fn_call_ctx_.contains(func_call.full_call_id)) {
        HLOG(FATAL) << "FuncCall already exists: "
                    << FuncCallHelper::DebugString(func_call);
    }
    fn_call_ctx_[func_call.full_call_id] = FnCallContext {
        .user_logspace = log_space,
        .metalog_progress = 0,
        .parent_call_id = protocol::kInvalidFuncCallId
    };
}

void EngineBase::OnNewInternalFuncCall(const FuncCall& func_call,
                                       const FuncCall& parent_func_call) {
    absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
    if (fn_call_ctx_.contains(func_call.full_call_id)) {
        HLOG(FATAL) << "FuncCall already exists: "
                    << FuncCallHelper::DebugString(func_call);
    }
    if (!fn_call_ctx_.contains(parent_func_call.full_call_id)) {
        HLOG(FATAL) << "Cannot find parent FuncCall: "
                    << FuncCallHelper::DebugString(parent_func_call);
    }
    FnCallContext ctx = fn_call_ctx_.at(parent_func_call.full_call_id);
    ctx.parent_call_id = parent_func_call.full_call_id;
    fn_call_ctx_[func_call.full_call_id] = std::move(ctx);
}

void EngineBase::OnFuncCallCompleted(const FuncCall& func_call) {
    absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
    if (!fn_call_ctx_.contains(func_call.full_call_id)) {
        HLOG(FATAL) << "Cannot find FuncCall: "
                    << FuncCallHelper::DebugString(func_call);
    }
    fn_call_ctx_.erase(func_call.full_call_id);
}

void EngineBase::LocalOpHandler(LocalOp* op) {
    switch (op->type) {
    case SharedLogOpType::APPEND:
        HandleLocalAppend(op);
        break;
    case SharedLogOpType::READ_NEXT:
        ABSL_FALLTHROUGH_INTENDED;
    case SharedLogOpType::READ_PREV:
        HandleLocalRead(op);
        break;
    case SharedLogOpType::TRIM:
        HandleLocalTrim(op);
        break;
    default:
        UNREACHABLE();
    }
}

void EngineBase::MessageHandler(const SharedLogMessage& message,
                                std::span<const char> payload) {
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::READ_NEXT:
        ABSL_FALLTHROUGH_INTENDED;
    case SharedLogOpType::READ_PREV:
        HandleRemoteRead(message);
        break;
    case SharedLogOpType::INDEX_DATA:
        OnRecvNewIndexData(message, payload);
        break;
    case SharedLogOpType::METALOGS:
        OnRecvNewMetaLogs(message, payload);
        break;
    case SharedLogOpType::RESPONSE:
        OnRecvResponse(message, payload);
        break;
    default:
        UNREACHABLE();
    }
}

void EngineBase::OnMessageFromFuncWorker(const Message& message) {
    protocol::FuncCall func_call = MessageHelper::GetFuncCall(message);
    FnCallContext ctx;
    {
        absl::ReaderMutexLock fn_ctx_lk(&fn_ctx_mu_);
        if (!fn_call_ctx_.contains(func_call.full_call_id)) {
            HLOG(ERROR) << "Cannot find FuncCall: "
                        << FuncCallHelper::DebugString(func_call);
            return;
        }
        ctx = fn_call_ctx_.at(func_call.full_call_id);
    }

    LocalOp* op = log_op_pool_.Get();
    op->id = next_local_op_id_.fetch_add(1, std::memory_order_acq_rel);
    op->start_timestamp = GetMonotonicMicroTimestamp();
    op->client_id = message.log_client_id;
    op->client_data = message.log_client_data;
    op->func_call_id = func_call.full_call_id;
    op->user_logspace = ctx.user_logspace;
    op->metalog_progress = ctx.metalog_progress;
    op->type = MessageHelper::GetSharedLogOpType(message);
    op->seqnum = kInvalidLogSeqNum;
    op->user_tag = kInvalidLogTag;
    op->data.Reset();

    switch (op->type) {
    case SharedLogOpType::APPEND:
        op->user_tag = message.log_tag;
        op->data.AppendData(MessageHelper::GetInlineData(message));
        break;
    case SharedLogOpType::READ_NEXT:
        ABSL_FALLTHROUGH_INTENDED;
    case SharedLogOpType::READ_PREV:
        op->user_tag = message.log_tag;
        op->seqnum = message.log_seqnum;
        break;
    case SharedLogOpType::TRIM:
        op->user_tag = message.log_tag;
        op->seqnum = message.log_seqnum;
        break;
    default:
        HLOG(FATAL) << "Unknown shared log op type: " << message.log_op;
    }

    LocalOpHandler(op);
}

void EngineBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                        const SharedLogMessage& message,
                                        std::span<const char> payload) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    DCHECK(
        (conn_type == kSequencerIngressTypeId && op_type == SharedLogOpType::METALOGS)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_NEXT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_PREV)
     || (conn_type == kStorageIngressTypeId && op_type == SharedLogOpType::INDEX_DATA)
     || op_type == SharedLogOpType::RESPONSE
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

void EngineBase::ReplicateLogEntry(const View* view, const LogMetaData& log_metadata,
                                   std::span<const char> log_data) {
    SharedLogMessage message = SharedLogMessageHelper::NewReplicateMessage();
    log_utils::PopulateMetaDataToMessage(log_metadata, &message);
    message.origin_node_id = node_id_;
    message.payload_size = gsl::narrow_cast<uint32_t>(log_data.size());
    const View::Engine* engine_node = view->GetEngineNode(node_id_);
    for (uint16_t storage_id : engine_node->GetStorageNodes()) {
        engine_->SendSharedLogMessage(protocol::ConnType::ENGINE_TO_STORAGE,
                                      storage_id, message, log_data);
    }
}

void EngineBase::FinishLocalOpWithResponse(LocalOp* op, Message* response,
                                           uint64_t metalog_progress) {
    if (metalog_progress > 0) {
        absl::MutexLock fn_ctx_lk(&fn_ctx_mu_);
        if (fn_call_ctx_.contains(op->func_call_id)) {
            FnCallContext& ctx = fn_call_ctx_[op->func_call_id];
            if (metalog_progress > ctx.metalog_progress) {
                ctx.metalog_progress = metalog_progress;
            }
        }
    }
    response->log_client_data = op->client_data;
    engine_->SendFuncWorkerMessage(op->client_id, response);
    log_op_pool_.Return(op);
}

void EngineBase::FinishLocalOpWithFailure(LocalOp* op, SharedLogResultType result,
                                          uint64_t metalog_progress) {
    Message response = MessageHelper::NewSharedLogOpFailed(result);
    FinishLocalOpWithResponse(op, &response, metalog_progress);
}

void EngineBase::LogCachePut(const LogMetaData& log_metadata,
                             std::span<const char> log_data) {
    if (log_cache_ == nullptr) {
        return;
    }
    HVLOG(1) << fmt::format("Store cache for log entry (seqnum {})",
                            bits::HexStr0x(log_metadata.seqnum));
    log_cache_->Put(log_metadata, log_data);
}

bool EngineBase::LogCacheGet(uint64_t seqnum, LogEntry* log_entry) {
    if (log_cache_ == nullptr) {
        return false;
    }
    return log_cache_->Get(seqnum, log_entry);
}

bool EngineBase::SendIndexReadRequest(const View::Sequencer* sequencer_node,
                                      SharedLogMessage* request) {
    static constexpr int kMaxRetries = 3;

    request->sequencer_id = sequencer_node->node_id();
    request->view_id = sequencer_node->view()->id();
    for (int i = 0; i < kMaxRetries; i++) {
        uint16_t engine_id = sequencer_node->PickIndexEngineNode();
        bool success = engine_->SendSharedLogMessage(
            protocol::ConnType::SLOG_ENGINE_TO_ENGINE, engine_id,
            *request, /* payload= */ EMPTY_CHAR_SPAN);
        if (success) {
            return true;
        }
    }
    return false;
}

bool EngineBase::SendStorageReadRequest(const IndexQueryResult& result) {
    static constexpr int kMaxRetries = 3;
    DCHECK(result.state == IndexQueryResult::kFound);

    uint64_t seqnum = result.found_result.seqnum;
    SharedLogMessage request = SharedLogMessageHelper::NewReadAtMessage(
        bits::HighHalf64(seqnum), bits::LowHalf64(seqnum));
    request.user_metalog_progress = result.metalog_progress;
    request.origin_node_id = result.original_query.origin_node_id;
    request.hop_times = result.original_query.hop_times + 1;
    request.client_data = result.original_query.client_data;
    for (int i = 0; i < kMaxRetries; i++) {
        uint16_t storage_id = result.found_result.engine_node->PickStorageNode();
        bool success = engine_->SendSharedLogMessage(
            protocol::ConnType::ENGINE_TO_STORAGE, storage_id,
            request, /* payload= */ EMPTY_CHAR_SPAN);
        if (success) {
            return true;
        }
    }
    return false;
}

void EngineBase::SendReadResponse(const IndexQuery& query,
                                  protocol::SharedLogMessage* response,
                                  std::span<const char> payload) {
    response->origin_node_id = node_id_;
    response->hop_times = query.hop_times + 1;
    response->client_data = query.client_data;
    response->payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    uint16_t engine_id = query.origin_node_id;
    bool success = engine_->SendSharedLogMessage(
        protocol::ConnType::SLOG_ENGINE_TO_ENGINE,
        engine_id, *response, payload);
    if (!success) {
        HLOG(WARNING) << fmt::format("Failed to send read response to engine {}",
                                     engine_id);
    }
}

void EngineBase::SendReadFailureResponse(const IndexQuery& query,
                                         protocol::SharedLogResultType result_type,
                                         uint64_t metalog_progress) {
    SharedLogMessage response = SharedLogMessageHelper::NewResponse(result_type);
    response.user_metalog_progress = metalog_progress;
    SendReadResponse(query, &response);
}

bool EngineBase::SendSequencerMessage(uint16_t sequencer_id,
                                      SharedLogMessage* message,
                                      std::span<const char> payload) {
    message->origin_node_id = node_id_;
    message->payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    return engine_->SendSharedLogMessage(
        protocol::ConnType::ENGINE_TO_SEQUENCER,
        sequencer_id, *message, payload);
}

server::IOWorker* EngineBase::SomeIOWorker() {
    return engine_->SomeIOWorker();
}

}  // namespace log
}  // namespace faas
