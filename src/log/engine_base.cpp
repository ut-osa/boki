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
        log_cache_.emplace(absl::GetFlag(FLAGS_slog_engine_cache_cap_mb));
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
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
        HandleLocalRead(op);
        break;
    case SharedLogOpType::TRIM:
        HandleLocalTrim(op);
        break;
    case SharedLogOpType::SET_AUXDATA:
        HandleLocalSetAuxData(op);
        break;
    default:
        UNREACHABLE();
    }
}

void EngineBase::MessageHandler(const SharedLogMessage& message,
                                std::span<const char> payload) {
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
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

void EngineBase::PopulateLogTagsAndData(LocalOp* op, std::span<const char> data) {
    DCHECK(op->type == SharedLogOpType::APPEND);
    size_t num_tags = op->user_tags.size();
    if (num_tags > 0) {
        memcpy(op->user_tags.data(), data.data(), num_tags * sizeof(uint64_t));
    }
    op->data.AppendData(data.subspan(num_tags * sizeof(uint64_t)));
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
    op->query_tag = kInvalidLogTag;
    op->user_tags.clear();
    op->data.Reset();

    switch (op->type) {
    case SharedLogOpType::APPEND:
        DCHECK_EQ(message.log_aux_data_size, 0U);
        op->user_tags.resize(message.log_num_tags);
        break;
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
        op->query_tag = message.log_tag;
        op->seqnum = message.log_seqnum;
        break;
    case SharedLogOpType::TRIM:
        op->seqnum = message.log_seqnum;
        break;
    case SharedLogOpType::SET_AUXDATA:
        op->seqnum = message.log_seqnum;
        break;
    default:
        HLOG(FATAL) << "Unknown shared log op type: " << message.log_op;
    }

    std::span<const char> data;
    std::string aux_buf;
    bool data_ok = false;
    if ((message.flags & protocol::kUseAuxBufferFlag) == 0) {
        data = MessageHelper::GetInlineData(message);
        data_ok = true;
    } else {
        uint64_t id = MessageHelper::GetAuxBufferId(message);
        VLOG_F(1, "Waiting for aux buffer (ID {})", bits::HexStr0x(id));
        absl::MutexLock lk(&request_for_buf_mu_);
        if (auto tmp = engine_->GrabAuxBuffer(id); tmp.has_value()) {
            aux_buf = std::move(*tmp);
            data = STRING_AS_SPAN(aux_buf);
            data_ok = true;
        } else {
            if (requests_for_buf_.contains(id)) {
                LOG(FATAL) << "Duplicated aux buffer ID";
            }
            requests_for_buf_[id] = op;
        }
    }

    if (!data_ok) {
        return;
    }
    switch (op->type) {
    case SharedLogOpType::APPEND:
        PopulateLogTagsAndData(op, data);
        break;
    case SharedLogOpType::SET_AUXDATA:
        op->data.AppendData(data);
        break;
    default:
        break;
    }

    LocalOpHandler(op);
}

void EngineBase::OnAuxBufferFromFuncWorker(uint64_t id) {
    LocalOp* op = nullptr;
    std::string aux_buf;
    {
        absl::MutexLock lk(&request_for_buf_mu_);
        if (requests_for_buf_.contains(id)) {
            op = requests_for_buf_.at(id);
            requests_for_buf_.erase(id);
            if (auto tmp = engine_->GrabAuxBuffer(id); tmp.has_value()) {
                aux_buf = std::move(*tmp);
            } else {
                UNREACHABLE();
            }
        }
    }
    if (op == nullptr) {
        return;
    }
    std::span<const char> data = STRING_AS_SPAN(aux_buf);
    switch (op->type) {
    case SharedLogOpType::APPEND:
        PopulateLogTagsAndData(op, data);
        break;
    case SharedLogOpType::SET_AUXDATA:
        op->data.AppendData(data);
        break;
    default:
        break;
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
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_NEXT_B)
     || (conn_type == kStorageIngressTypeId && op_type == SharedLogOpType::INDEX_DATA)
     || op_type == SharedLogOpType::RESPONSE
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

void EngineBase::ReplicateLogEntry(const View* view, const LogMetaData& log_metadata,
                                   std::span<const uint64_t> user_tags,
                                   std::span<const char> log_data) {
    SharedLogMessage message = SharedLogMessageHelper::NewReplicateMessage();
    log_utils::PopulateMetaDataToMessage(log_metadata, &message);
    message.origin_node_id = node_id_;
    message.payload_size = gsl::narrow_cast<uint32_t>(
        user_tags.size() * sizeof(uint64_t) + log_data.size());
    const View::Engine* engine_node = view->GetEngineNode(node_id_);
    for (uint16_t storage_id : engine_node->GetStorageNodes()) {
        engine_->SendSharedLogMessage(protocol::ConnType::ENGINE_TO_STORAGE,
                                      storage_id, message,
                                      VECTOR_AS_CHAR_SPAN(user_tags), log_data);
    }
}

void EngineBase::PropagateAuxData(const View* view, const LogMetaData& log_metadata, 
                                  std::span<const char> aux_data) {
    uint16_t engine_id = gsl::narrow_cast<uint16_t>(
        bits::HighHalf64(log_metadata.localid));
    DCHECK(view->contains_engine_node(engine_id));
    const View::Engine* engine_node = view->GetEngineNode(engine_id);
    SharedLogMessage message = SharedLogMessageHelper::NewSetAuxDataMessage(
        log_metadata.seqnum);
    message.origin_node_id = node_id_;
    message.payload_size = gsl::narrow_cast<uint32_t>(aux_data.size());
    for (uint16_t storage_id : engine_node->GetStorageNodes()) {
        engine_->SendSharedLogMessage(protocol::ConnType::ENGINE_TO_STORAGE,
                                      storage_id, message, aux_data);
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

bool EngineBase::SendFuncWorkerAuxBuffer(uint16_t client_id,
                                         uint64_t buf_id, std::span<const char> data) {
    VLOG(1) << "Will send aux buffer with ID " << bits::HexStr0x(buf_id);
    return engine_->SendFuncWorkerAuxBuffer(client_id, buf_id, data);
}

void EngineBase::LogCachePut(const LogMetaData& log_metadata,
                             std::span<const uint64_t> user_tags,
                             std::span<const char> log_data) {
    if (!log_cache_.has_value()) {
        return;
    }
    HVLOG_F(1, "Store cache for log entry (seqnum {})", bits::HexStr0x(log_metadata.seqnum));
    log_cache_->Put(log_metadata, user_tags, log_data);
}

std::optional<LogEntry> EngineBase::LogCacheGet(uint64_t seqnum) {
    return log_cache_.has_value() ? log_cache_->Get(seqnum) : std::nullopt;
}

void EngineBase::LogCachePutAuxData(uint64_t seqnum, std::span<const char> data) {
    if (log_cache_.has_value()) {
        log_cache_->PutAuxData(seqnum, data);
    }
}

std::optional<std::string> EngineBase::LogCacheGetAuxData(uint64_t seqnum) {
    return log_cache_.has_value() ? log_cache_->GetAuxData(seqnum) : std::nullopt;
}

bool EngineBase::SendIndexReadRequest(const View::Sequencer* sequencer_node,
                                      SharedLogMessage* request) {
    static constexpr int kMaxRetries = 3;

    request->sequencer_id = sequencer_node->node_id();
    request->view_id = sequencer_node->view()->id();
    for (int i = 0; i < kMaxRetries; i++) {
        uint16_t engine_id = sequencer_node->PickIndexEngineNode();
        if (engine_id == node_id_) {
            continue;
        }
        bool success = engine_->SendSharedLogMessage(
            protocol::ConnType::SLOG_ENGINE_TO_ENGINE, engine_id, *request);
        if (success) {
            return true;
        }
    }
    return false;
}

bool EngineBase::SendStorageReadRequest(const IndexQueryResult& result,
                                        const View::Engine* engine_node) {
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
        uint16_t storage_id = engine_node->PickStorageNode();
        bool success = engine_->SendSharedLogMessage(
            protocol::ConnType::ENGINE_TO_STORAGE, storage_id, request);
        if (success) {
            return true;
        }
    }
    return false;
}

void EngineBase::SendReadResponse(const IndexQuery& query,
                                  protocol::SharedLogMessage* response,
                                  std::span<const char> user_tags_payload,
                                  std::span<const char> data_payload,
                                  std::span<const char> aux_data_payload) {
    response->origin_node_id = node_id_;
    response->hop_times = query.hop_times + 1;
    response->client_data = query.client_data;
    response->payload_size = gsl::narrow_cast<uint32_t>(
        user_tags_payload.size() + data_payload.size() + aux_data_payload.size());
    uint16_t engine_id = query.origin_node_id;
    bool success = engine_->SendSharedLogMessage(
        protocol::ConnType::SLOG_ENGINE_TO_ENGINE,
        engine_id, *response, user_tags_payload, data_payload, aux_data_payload);
    if (!success) {
        HLOG_F(WARNING, "Failed to send read response to engine {}", engine_id);
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

uint64_t EngineBase::NextAuxBufferId() {
    return engine_->NextAuxBufferId();
}

}  // namespace log
}  // namespace faas
