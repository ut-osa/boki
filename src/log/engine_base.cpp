#include "log/engine_base.h"

#include "common/time.h"
#include "log/flags.h"
#include "log/utils.h"
#include "server/constants.h"
#include "engine/engine.h"
#include "utils/bits.h"

#define LOG_HEADER "LogEngineBase: "

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
      next_local_op_id_(0),
      append_counter_(stat::Counter::StandardReportCallback("log_append"), "sharedlog"),
      read_counter_(stat::Counter::StandardReportCallback("log_read"), "sharedlog"),
      trim_counter_(stat::Counter::StandardReportCallback("log_trim"), "sharedlog"),
      setaux_counter_(stat::Counter::StandardReportCallback("log_set_auxdata"), "sharedlog"),
      append_delay_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          "log_append_delay"), "sharedlog"),
      read_delay_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          "log_read_delay"), "sharedlog"),
      trim_delay_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          "log_trim_delay"), "sharedlog"),
      setaux_delay_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          "log_set_auxdata_delay"), "sharedlog") {}

EngineBase::~EngineBase() {}

zk::ZKSession* EngineBase::zk_session() {
    return engine_->zk_session();
}

void EngineBase::Start() {
    SetupZKWatchers();
    // Setup cache
    if (absl::GetFlag(FLAGS_slog_engine_enable_cache)) {
        log_cache_.emplace(absl::GetFlag(FLAGS_slog_engine_cache_cap_mb));
    }
    // Set init function for LocalOp
    log_op_pool_.SetObjectInitFn([] (LocalOp* op) {
        op->type = SharedLogOpType::INVALID;
        op->client_id = 0;
        op->user_logspace = 0;
        op->id = 0;
        op->client_data = 0;
        op->metalog_progress = 0;
        op->query_tag = kInvalidLogTag;
        op->seqnum = kInvalidLogSeqNum;
        op->func_call_id = protocol::kInvalidFuncCallId;
        op->start_timestamp = -1;
        op->user_tags.clear();
        op->data.Reset();
    });
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

void EngineBase::TickCounter(SharedLogOpType op_type) {
    absl::MutexLock lk(&stat_mu_);
    switch (op_type) {
    case SharedLogOpType::APPEND:
        append_counter_.Tick();
        break;
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
        read_counter_.Tick();
        break;
    case SharedLogOpType::TRIM:
        trim_counter_.Tick();
        break;
    case SharedLogOpType::SET_AUXDATA:
        setaux_counter_.Tick();
        break;
    default:
        UNREACHABLE();
    }
}

void EngineBase::RecordOpDelay(protocol::SharedLogOpType op_type, int32_t delay) {
    absl::MutexLock lk(&stat_mu_);
    switch (op_type) {
    case SharedLogOpType::APPEND:
        append_delay_stat_.AddSample(delay);
        break;
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
        read_delay_stat_.AddSample(delay);
        break;
    case SharedLogOpType::TRIM:
        trim_delay_stat_.AddSample(delay);
        break;
    case SharedLogOpType::SET_AUXDATA:
        setaux_delay_stat_.AddSample(delay);
        break;
    default:
        UNREACHABLE();
    }
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
        ResponseHandler(message, payload);
        break;
    default:
        UNREACHABLE();
    }
}

void EngineBase::ResponseHandler(const SharedLogMessage& message,
                                 std::span<const char> payload) {
    SharedLogResultType result_type = SharedLogMessageHelper::GetResultType(message);
    switch (result_type) {
    case SharedLogResultType::READ_OK:
    case SharedLogResultType::EMPTY:
    case SharedLogResultType::READ_TRIMMED:
    case SharedLogResultType::DATA_LOST:
        OnRecvReadResponse(result_type, message, payload);
        break;
    case SharedLogResultType::TRIM_OK:
    case SharedLogResultType::TRIM_FAILED:
        DCHECK(payload.empty());
        OnRecvTrimResponse(result_type, message);
        break;
    default:
        HLOG(FATAL) << "Unknown result type: " << message.op_result;
    }
}

void EngineBase::PopulateLogTagsAndData(const Message& message, LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::APPEND);
    DCHECK_EQ(message.log_aux_data_size, 0U);
    std::span<const char> data = MessageHelper::GetInlineData(message);
    size_t num_tags = message.log_num_tags;
    if (num_tags > 0) {
        op->user_tags.resize(num_tags);
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
    DCHECK_EQ(op->seqnum, kInvalidLogSeqNum);
    DCHECK_EQ(op->query_tag, kInvalidLogTag);
    DCHECK(op->user_tags.empty());
    DCHECK(op->data.empty());

    switch (op->type) {
    case SharedLogOpType::APPEND:
        PopulateLogTagsAndData(message, op);
        break;
    case SharedLogOpType::READ_NEXT:
    case SharedLogOpType::READ_PREV:
    case SharedLogOpType::READ_NEXT_B:
        op->query_tag = message.log_tag;
        op->seqnum = message.log_seqnum;
        break;
    case SharedLogOpType::TRIM:
        op->trim_tag = message.log_tag;
        op->seqnum = message.log_seqnum;
        break;
    case SharedLogOpType::SET_AUXDATA:
        op->seqnum = message.log_seqnum;
        op->data.AppendData(MessageHelper::GetInlineData(message));
        break;
    default:
        HLOG(FATAL) << "Unknown shared log op type: " << message.log_op;
    }

    TickCounter(op->type);
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
                HVLOG_F(1, "Update metalog progress from {} to {} for func call {}",
                        bits::HexStr0x(ctx.metalog_progress),
                        bits::HexStr0x(metalog_progress),
                        bits::HexStr0x(op->func_call_id));
                ctx.metalog_progress = metalog_progress;
            }
        }
    }
    response->log_client_data = op->client_data;
    engine_->SendFuncWorkerMessage(op->client_id, response);
    int32_t op_delay = gsl::narrow_cast<int32_t>(
        GetMonotonicMicroTimestamp() - op->start_timestamp);
    RecordOpDelay(op->type, op_delay);
    log_op_pool_.Return(op);
}

void EngineBase::FinishLocalOpWithFailure(LocalOp* op, SharedLogResultType result,
                                          uint64_t metalog_progress) {
    Message response = MessageHelper::NewSharedLogOpFailed(result);
    FinishLocalOpWithResponse(op, &response, metalog_progress);
}

void EngineBase::LogCachePut(const LogMetaData& log_metadata,
                             std::span<const uint64_t> user_tags,
                             std::span<const char> log_data) {
    if (!log_cache_.has_value()) {
        return;
    }
    HVLOG_F(1, "Store cache for log entry (seqnum {})", bits::HexStr0x(log_metadata.seqnum));
    log_cache_->PutBySeqnum(log_metadata, user_tags, log_data);
}

std::optional<LogEntry> EngineBase::LogCacheGet(uint64_t seqnum) {
    return log_cache_.has_value() ? log_cache_->GetBySeqnum(seqnum) : std::nullopt;
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
    request.hop_times = result.original_query.hop_times;
    request.hop_times++;
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
    response->hop_times = query.hop_times;
    response->hop_times++;
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

}  // namespace log
}  // namespace faas
