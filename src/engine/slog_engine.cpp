#include "engine/slog_engine.h"

#include "common/time.h"
#include "log/common.h"
#include "server/constants.h"
#include "engine/flags.h"
#include "engine/sequencer_connection.h"
#include "engine/engine.h"

#define log_header_ "SLogEngine: "

ABSL_FLAG(bool, slog_engine_fast_append, false, "");

namespace faas {
namespace engine {

using log::LogRecord;
using log::EngineCore;
using log::IndexQuery;
using log::IndexResult;

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;
using protocol::SequencerMessage;
using protocol::SequencerMessageHelper;

SLogEngine::SLogEngine(Engine* engine)
    : engine_(engine),
      sequencer_config_(&engine->sequencer_config_),
      core_(engine->node_id()),
      next_op_id_(1),
      completed_actions_(nullptr) {
    core_.SetLogPersistedCallback(
        absl::bind_front(&SLogEngine::LogPersisted, this));
    core_.SetLogDiscardedCallback(
        absl::bind_front(&SLogEngine::LogDiscarded, this));
    core_.SetSendTagVecCallback(
        absl::bind_front(&SLogEngine::SendTagVec, this));
    // std::string storage_backend = absl::GetFlag(FLAGS_slog_storage_backend);
    // if (storage_backend == "inmem") {
    //     storage_.reset(new log::InMemoryStorage());
    // } else if (storage_backend == "rocksdb") {
    //     std::string db_path = absl::GetFlag(FLAGS_slog_storage_datadir);
    //     CHECK(!db_path.empty()) << "Empty slog_storage_datadir";
    //     storage_.reset(new log::RocksDBStorage(db_path));
    // } else {
    //     HLOG(FATAL) << "Unknown storage backend: " << storage_backend;
    // }
    for (int i = 0; i < EngineCore::kTotalProgressKinds; i++) {
        known_fsm_progress_[i].store(0);
    }
    SetupTimers();
}

SLogEngine::~SLogEngine() {}

uint16_t SLogEngine::my_node_id() const {
    return engine_->node_id();
}

void SLogEngine::OnNewExternalFuncCall(const protocol::FuncCall& func_call,
                                       uint32_t log_space) {
    absl::MutexLock lk(&func_ctx_mu_);
    uint64_t call_id = func_call.full_call_id;
    DCHECK(!func_call_ctx_.contains(call_id));
    FuncCallContext ctx = {
        .log_space = log_space,
        .fsm_progress = 0,
        .parent_call_id = protocol::kInvalidFuncCallId,
    };
    func_call_ctx_[call_id] = std::move(ctx);
}

void SLogEngine::OnNewInternalFuncCall(const protocol::FuncCall& func_call,
                                       const protocol::FuncCall& parent_func_call) {
    absl::MutexLock lk(&func_ctx_mu_);
    uint64_t parent_call_id = parent_func_call.full_call_id;
    DCHECK(func_call_ctx_.contains(parent_call_id));
    uint64_t call_id = func_call.call_id;
    DCHECK(!func_call_ctx_.contains(call_id));
    func_call_ctx_[call_id] = func_call_ctx_[parent_call_id];
    func_call_ctx_[call_id].parent_call_id = parent_call_id;
}

void SLogEngine::OnFuncCallCompleted(const protocol::FuncCall& func_call) {
    absl::MutexLock lk(&func_ctx_mu_);
    uint64_t call_id = func_call.full_call_id;
    DCHECK(func_call_ctx_.contains(call_id));
    const FuncCallContext& ctx = func_call_ctx_[call_id];
    if (ctx.parent_call_id != protocol::kInvalidFuncCallId
            && func_call_ctx_.contains(ctx.parent_call_id)) {
        FuncCallContext& parent_ctx = func_call_ctx_[ctx.parent_call_id];
        DCHECK_EQ(ctx.log_space, parent_ctx.log_space);
        parent_ctx.fsm_progress = std::max(parent_ctx.fsm_progress, ctx.fsm_progress);
    }
    func_call_ctx_.erase(call_id);
}

SLogEngine::FuncCallContext SLogEngine::GetFuncContext(const protocol::FuncCall& func_call) {
    absl::ReaderMutexLock lk(&func_ctx_mu_);
    uint64_t call_id = func_call.full_call_id;
    DCHECK(func_call_ctx_.contains(call_id));
    return func_call_ctx_[call_id];
}

void SLogEngine::UpdateFuncFsmProgress(const protocol::FuncCall& func_call,
                                       uint32_t fsm_progress) {
    absl::MutexLock lk(&func_ctx_mu_);
    uint64_t call_id = func_call.full_call_id;
    DCHECK(func_call_ctx_.contains(call_id));
    FuncCallContext& ctx = func_call_ctx_[call_id];
    ctx.fsm_progress = std::max(ctx.fsm_progress, fsm_progress);
}

namespace {
inline void FillReadLogResponse(uint64_t seqnum, const LogRecord& record,
                                uint32_t fsm_progress, protocol::Message* response) {
    if (record.data.size() > MESSAGE_INLINE_DATA_SIZE) {
        LOG(FATAL) << "Log data too long to fit into one message, "
                      "this should not happend given current implementation";
    }
    *response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::READ_OK, seqnum);
    response->log_fsm_progress = fsm_progress;
    response->log_seqnum = record.seqnum;
    response->log_tag = record.tag;
    MessageHelper::SetInlineData(response, record.data);
}
}

void SLogEngine::SetupTimers() {
    engine_->CreatePeriodicTimer(
        kSLogLocalCutTimerTypeId, EngineCore::local_cut_interval(),
        absl::bind_front(&SLogEngine::LocalCutTimerTriggered, this));
    engine_->CreatePeriodicTimer(
        kSLogStateCheckTimerTypeId,
        absl::Seconds(absl::GetFlag(FLAGS_slog_statecheck_interval_sec)),
        absl::bind_front(&SLogEngine::StateCheckTimerTriggered, this));
}

void SLogEngine::LocalCutTimerTriggered() {
    mu_.AssertNotHeld();
    log::LocalCutMsgProto message;
    {
        absl::MutexLock lk(&mu_);
        if (!core_.BuildLocalCutMessage(&message)) {
            return;
        }
    }
    std::string serialized_message;
    message.SerializeToString(&serialized_message);
    std::span<const char> data(serialized_message.data(), serialized_message.length());
    SendSequencerMessage(SequencerMessageHelper::NewLocalCut(data), data);
}

void SLogEngine::StateCheckTimerTriggered() {
    mu_.AssertNotHeld();
    DoStateCheck();
}

void SLogEngine::OnSequencerMessage(const SequencerMessage& message,
                                    std::span<const char> payload) {
    if (!SequencerMessageHelper::IsFsmRecords(message)) {
        HLOG(FATAL) << fmt::format("Unknown message type: {}!", message.message_type);
    }
    log::FsmRecordsMsgProto message_proto;
    if (!message_proto.ParseFromArray(payload.data(), payload.size())) {
        HLOG(FATAL) << "Failed to parse sequencer message!";
        return;
    }
    CompletionActionVec completed_actions;
    uint32_t fsm_progress[EngineCore::kTotalProgressKinds];
    {
        absl::MutexLock lk(&mu_);
        DCHECK(completed_actions_ == nullptr);
        completed_actions_ = &completed_actions;
        core_.OnNewFsmRecordsMessage(message_proto);
        completed_actions_ = nullptr;
        for (int i = 0; i < EngineCore::kTotalProgressKinds; i++) {
            fsm_progress[i] = core_.fsm_progress(static_cast<EngineCore::FsmProgressKind>(i));
        }
    }
    for (const CompletionAction& action : completed_actions) {
        switch (action.type) {
        case CompletionAction::kLogPersisted:
            if (action.op != nullptr) {
                Message response = MessageHelper::NewSharedLogOpSucceeded(
                    SharedLogResultType::APPEND_OK, action.seqnum);
                response.log_fsm_progress = fsm_progress[EngineCore::kStorageProgress];
                FinishLogOp(action.op, &response);
            }
            break;
        case CompletionAction::kLogDiscarded:
            HLOG(WARNING) << fmt::format("Log with localid {:#018x} discarded", action.localid);
            if (action.op != nullptr) {
                RetryAppendOpIfDoable(action.op);
            }
            break;
        case CompletionAction::kSendTagVec:
            {
                Message message = MessageHelper::NewSharedLogIndexData(
                    action.seqnum,
                    std::span<const uint64_t>(action.tags.data(), action.tags.size()));
                action.view->ForEachNode([this, &message] (size_t, uint16_t node_id) {
                    if (node_id != my_node_id()) {
                        SendMessageToEngine(node_id, &message);
                    }
                });
            }
            break;
        default:
            UNREACHABLE();
        }
    }
    for (int i = 0; i < EngineCore::kTotalProgressKinds; i++) {
        AdvanceFsmProgress(static_cast<EngineCore::FsmProgressKind>(i), fsm_progress[i]);
    }
}

SLogEngine::LogOp* SLogEngine::AllocLogOp(LogOpType type, uint32_t log_space,
                                          uint32_t min_fsm_progress,
                                          uint16_t client_id, uint64_t client_data) {
    LogOp* op = log_op_pool_.Get();
    op->id = next_op_id_.fetch_add(1, std::memory_order_relaxed);
    op->id = (op->id << 8) + uint16_t{type};
    op->log_space = log_space;
    op->min_fsm_progress = min_fsm_progress;
    op->client_id = client_id;
    op->src_node_id = my_node_id();
    op->client_data = client_data;
    op->log_tag = protocol::kInvalidLogTag;
    op->log_seqnum = protocol::kInvalidLogSeqNum;
    op->log_data.Reset();
    op->func_call = protocol::kInvalidFuncCall;
    op->remaining_retries = kMaxRetires;
    op->start_timestamp = GetMonotonicMicroTimestamp();
    op->hop_times = 0;
    return op;
}

void SLogEngine::OnMessageFromOtherEngine(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::RESPONSE) {
        RemoteOpFinished(message);
        return;
    }
    HandleRemoteRequest(message);
}

void SLogEngine::OnMessageFromFuncWorker(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    FuncCallContext ctx = GetFuncContext(MessageHelper::GetFuncCall(message));
    HandleLocalRequest(ctx, message);
}

void SLogEngine::HandleRemoteRequest(const protocol::Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        HandleRemoteAppend(message);
    } else if (op_type == SharedLogOpType::REPLICATE) {
        HandleRemoteReplicate(message);
    } else if (op_type == SharedLogOpType::INDEX_DATA) {
        HandleRemoteIndexData(message);
    } else if (op_type == SharedLogOpType::READ_AT) {
        HandleRemoteReadAt(message);
    } else {
        HLOG(FATAL) << "Unknown SharedLogOpType from remote message: "
                    << static_cast<uint16_t>(op_type);
    }
}

void SLogEngine::HandleRemoteAppend(const protocol::Message& message) {
    DCHECK(MessageHelper::GetSharedLogOpType(message) == SharedLogOpType::APPEND);
    DCHECK(message.src_node_id != my_node_id());
    std::span<const char> data = MessageHelper::GetInlineData(message);
    LogOp* op = AllocLogOp(LogOpType::kAppend, message.log_space, message.log_fsm_progress,
                           /* client_id= */ 0, message.log_client_data);
    op->log_tag = message.log_tag;
    op->src_node_id = message.src_node_id;
    op->hop_times = message.hop_times;
    NewAppendLogOp(op, data);
}

void SLogEngine::HandleRemoteReplicate(const Message& message) {
    DCHECK(MessageHelper::GetSharedLogOpType(message) == SharedLogOpType::REPLICATE);
    DCHECK(message.src_node_id != my_node_id());
    absl::MutexLock lk(&mu_);
    core_.StoreLogAsBackupNode(message.log_tag, MessageHelper::GetInlineData(message),
                               message.log_localid);
}

void SLogEngine::HandleRemoteIndexData(const protocol::Message& message) {
    DCHECK(MessageHelper::GetSharedLogOpType(message) == SharedLogOpType::INDEX_DATA);
    DCHECK(message.src_node_id != my_node_id());
    uint64_t start_seqnum = message.log_seqnum;
    uint16_t primary_node_id = message.src_node_id;
    std::span<const char> data = MessageHelper::GetInlineData(message);
    DCHECK(data.size() % sizeof(uint64_t) == 0);
    log::TagIndex::TagVec tags(data.size() / sizeof(uint64_t));
    memcpy(tags.data(), data.data(), data.size());
    uint32_t fsm_progress;
    {
        absl::MutexLock lk(&mu_);
        core_.OnRecvTagData(primary_node_id, start_seqnum, tags);
        fsm_progress = core_.fsm_progress(EngineCore::kIndexProgress);
    }
    AdvanceFsmProgress(EngineCore::kIndexProgress, fsm_progress);
}

void SLogEngine::HandleRemoteReadAt(const protocol::Message& message) {
    DCHECK(MessageHelper::GetSharedLogOpType(message) == SharedLogOpType::READ_AT);
    DCHECK(message.src_node_id != my_node_id());
    if (!CheckForFsmProgress(EngineCore::kStorageProgress, nullptr, message)) {
        return;
    }
    Message response;
    ReadLogData(message.log_seqnum, message.log_fsm_progress, &response);
    response.log_client_data = message.log_client_data;
    response.hop_times = message.hop_times + 1;
    SendMessageToEngine(message.src_node_id, &response);
}

#if 0
void SLogEngine::HandleRemoteRead(const protocol::Message& message) {
    SharedLogOpType msg_type = MessageHelper::GetSharedLogOpType(message);
    DCHECK(msg_type == SharedLogOpType::READ_NEXT || msg_type == SharedLogOpType::READ_PREV);
    DCHECK(message.log_tag != log::kEmptyLogTag);
    LogOp* op = nullptr;
    LogOpType type = (msg_type == SharedLogOpType::READ_NEXT) ? LogOpType::kReadNext
                                                              : LogOpType::kReadPrev;
    if (message.src_node_id == my_node_id()) {
        VLOG(1) << "Remote read request routes back to myself";
        {
            absl::MutexLock lk(&mu_);
            op = GrabLogOp(remote_ops_, message.log_client_data);
        }
        if (op == nullptr) {
            return;
        }
        DCHECK_EQ(type, op_type(op));
        DCHECK_EQ(message.log_tag, op->log_tag);
        DCHECK((type == LogOpType::kReadNext && message.log_seqnum > op->log_seqnum)
               || (type == LogOpType::kReadPrev && message.log_seqnum < op->log_seqnum));
        op->log_seqnum = message.log_seqnum;
        op->hop_times = message.hop_times;
    } else {
        op = AllocLogOp(type, message.log_space, message.log_fsm_progress,
                        /* client_id= */ 0, message.log_client_data);
        op->log_tag = message.log_tag;
        op->log_seqnum = message.log_seqnum;
        op->src_node_id = message.src_node_id;
        op->hop_times = message.hop_times;
    }
    NewReadLogOp(op);
}
#endif

void SLogEngine::HandleLocalRequest(const FuncCallContext& ctx,
                                    const protocol::Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        HandleLocalAppend(ctx, message);
    } else if (op_type == SharedLogOpType::READ_NEXT) {
        HandleLocalRead(ctx, message, /* direction= */ 1);
    } else if (op_type == SharedLogOpType::READ_PREV) {
        HandleLocalRead(ctx, message, /* direction= */ -1);
    } else if (op_type == SharedLogOpType::TRIM) {
        // TODO
        NOT_IMPLEMENTED();
    } else {
        HLOG(FATAL) << "Unknown SharedLogOpType from local message: "
                    << static_cast<uint16_t>(op_type);
    }
}

void SLogEngine::HandleLocalAppend(const FuncCallContext& ctx, const Message& message) {
    std::span<const char> data = MessageHelper::GetInlineData(message);
    if (data.empty()) {
        SendFailedResponse(message, SharedLogResultType::BAD_ARGS);
        return;
    }
    protocol::FuncCall func_call = MessageHelper::GetFuncCall(message);
    LogOp* op = AllocLogOp(LogOpType::kAppend, ctx.log_space, ctx.fsm_progress,
                           message.log_client_id, message.log_client_data);
    op->func_call = func_call;
    op->log_tag = message.log_tag;
    if (op->log_tag == log::kEmptyLogTag) {
        HVLOG(1) << "Local append with default tag";
    } else {
        HVLOG(1) << fmt::format("Local append with tag {}", op->log_tag);
    }
    op->log_data.ResetWithData(data);
    NewAppendLogOp(op, data);
}

void SLogEngine::HandleLocalRead(const FuncCallContext& ctx,
                                 const protocol::Message& message, int direction) {
    DCHECK(direction != 0);
    if (message.log_tag == log::kEmptyLogTag) {
        NOT_IMPLEMENTED();
    }
    if (!CheckForFsmProgress(EngineCore::kIndexProgress, &ctx, message)) {
        return;
    }
    HVLOG(1) << fmt::format("Local read[{}] with tag {}", direction, message.log_tag);
    IndexResult result;
    uint32_t fsm_progress;
    const log::Fsm::View* view;
    {
        absl::ReaderMutexLock lk(&mu_);
        if (direction > 0) {
            result = core_.tag_index()->FindFirst(IndexQuery {
                .tag = message.log_tag,
                .start_seqnum = message.log_seqnum,
                .end_seqnum = log::kMaxLogSeqNum
            });
        } else {
            result = core_.tag_index()->FindFirst(IndexQuery {
                .tag = message.log_tag,
                .start_seqnum = 0,
                .end_seqnum = message.log_seqnum + 1
            });
        }
        if (result.seqnum != log::kInvalidLogSeqNum) {
            fsm_progress = core_.fsm_progress(EngineCore::kIndexProgress);
            view = core_.fsm()->view_with_id(log::SeqNumToViewId(result.seqnum));
            DCHECK(view != nullptr);
        }
    }
    if (result.seqnum == log::kInvalidLogSeqNum) {
        SendFailedResponse(message, SharedLogResultType::EMPTY);
        return;
    }
    LogOp* op = AllocLogOp(LogOpType::kReadAt, ctx.log_space, fsm_progress,
                            message.log_client_id, message.log_client_data);
    op->func_call = MessageHelper::GetFuncCall(message);
    op->log_seqnum = result.seqnum;
    NewReadAtLogOp(op, view, result.primary_node_id);
}

void SLogEngine::RemoteOpFinished(const protocol::Message& response) {
    LogOp* op = nullptr;
    {
        absl::MutexLock lk(&mu_);
        op = GrabLogOp(remote_ops_, response.log_client_data);
    }
    if (op == nullptr) {
        return;
    }
    DCHECK_EQ(op->src_node_id, my_node_id());
    op->hop_times = response.hop_times;
    switch (op_type(op)) {
    case LogOpType::kAppend:
        RemoteAppendFinished(response, op);
        break;
    case LogOpType::kReadAt:
        RemoteReadAtFinished(response, op);
        break;
    case LogOpType::kReadNext:
    case LogOpType::kReadPrev:
        RemoteReadFinished(response, op);
        break;
    default:
        UNREACHABLE();
    }
}

void SLogEngine::RemoteAppendFinished(const protocol::Message& message, LogOp* op) {
    SharedLogResultType result = MessageHelper::GetSharedLogResultType(message);
    if (result == SharedLogResultType::APPEND_OK) {
        Message response = message;
        FinishLogOp(op, &response);
    } else if (result == SharedLogResultType::LOCALID) {
        uint64_t localid = message.log_localid;
        uint64_t seqnum;
        uint32_t fsm_progress;
        bool known;
        {
            absl::MutexLock lk(&mu_);
            fsm_progress = core_.fsm_progress(EngineCore::kStorageProgress);
            known = core_.fsm()->ConvertLocalId(localid, &seqnum);
            if (!known) {
                core_.AddWaitForReplication(op->log_tag, localid);
                append_ops_[localid] = op;
            }
        }
        if (known) {
            if (seqnum != protocol::kInvalidLogSeqNum) {
                Message response = MessageHelper::NewSharedLogOpSucceeded(
                    SharedLogResultType::APPEND_OK, seqnum);
                response.log_fsm_progress = fsm_progress;
                FinishLogOp(op, &response);
            } else {
                RetryAppendOpIfDoable(op);
            }
        }
    } else if (result == SharedLogResultType::DISCARDED) {
        RetryAppendOpIfDoable(op);
    } else {
        HLOG(FATAL) << "Unknown response type: " << static_cast<uint16_t>(result);
    }
}

void SLogEngine::RemoteReadAtFinished(const protocol::Message& message, LogOp* op) {
    UpdateFuncFsmProgress(op->func_call, message.log_fsm_progress);
    Message response = message;
    response.log_seqnum = op->log_seqnum;
    FinishLogOp(op, &response);
}

void SLogEngine::RemoteReadFinished(const protocol::Message& message, LogOp* op) {
    UpdateFuncFsmProgress(op->func_call, message.log_fsm_progress);
    Message response = message;
    FinishLogOp(op, &response);
}

void SLogEngine::LogPersisted(uint64_t localid, uint64_t seqnum) {
    mu_.AssertHeld();
    DCHECK(completed_actions_ != nullptr);
    completed_actions_->push_back(CompletionAction {
        .type = CompletionAction::kLogPersisted,
        .localid = localid,
        .seqnum = seqnum,
        .op = GrabLogOp(append_ops_, localid)
    });
}

void SLogEngine::LogDiscarded(uint64_t localid) {
    mu_.AssertHeld();
    DCHECK(completed_actions_ != nullptr);
    completed_actions_->push_back(CompletionAction {
        .type = CompletionAction::kLogDiscarded,
        .localid = localid,
        .op = GrabLogOp(append_ops_, localid)
    });
}

void SLogEngine::SendTagVec(const log::Fsm::View* view, uint64_t start_seqnum,
                            const log::TagIndex::TagVec& tags) {
    mu_.AssertHeld();
    DCHECK(completed_actions_ != nullptr);
    completed_actions_->push_back(CompletionAction {
        .type = CompletionAction::kSendTagVec,
        .seqnum = start_seqnum,
        .view = view,
        .tags = tags
    });
}

void SLogEngine::FinishLogOp(LogOp* op, protocol::Message* response) {
    if (response != nullptr) {
        response->log_client_data = op->client_data;
        if (op->src_node_id == my_node_id()) {
            if (response->log_fsm_progress > 0) {
                UpdateFuncFsmProgress(op->func_call, response->log_fsm_progress);
            }
            RecordLogOpCompletion(op);
            engine_->SendFuncWorkerMessage(op->client_id, response);
        } else {
            response->hop_times = ++op->hop_times;
            SendMessageToEngine(op->src_node_id, response);
        }
    }
    log_op_pool_.Return(op);
}

void SLogEngine::ForwardLogOp(LogOp* op, uint16_t dst_node_id, protocol::Message* message) {
    DCHECK(message != nullptr);
    message->log_client_data = op->client_data;
    SendMessageToEngine(op->src_node_id, dst_node_id, message);
    log_op_pool_.Return(op);
}

void SLogEngine::NewAppendLogOp(LogOp* op, std::span<const char> data) {
    DCHECK(op_type(op) == LogOpType::kAppend);
    DCHECK(!data.empty());
    const log::Fsm::View* view = nullptr;
    uint16_t primary_node_id;
    uint64_t localid;
    bool success = false;
    bool fast_append = absl::GetFlag(FLAGS_slog_engine_fast_append);
    do {
        absl::MutexLock lk(&mu_);
        success = core_.PickPrimaryNodeForNewLog(op->log_tag, &primary_node_id);
        if (!success) {
            break;
        }
        if (primary_node_id == my_node_id()) {
            success = core_.StoreLogAsPrimaryNode(op->log_tag, data, &localid);
            if (success) {
                HVLOG(1) << fmt::format("New log stored (localid {:#018x})", localid);
                view = core_.fsm()->view_with_id(log::LocalIdToViewId(localid));
                DCHECK(view != nullptr);
                if (!fast_append || op->src_node_id == my_node_id()) {
                    append_ops_[localid] = op;
                }
            }
        } else {
            HVLOG(1) << fmt::format("Will append the new log to remote node {}", primary_node_id);
            if (op->src_node_id != my_node_id()) {
                success = false;
            } else {
                remote_ops_[op->id] = op;
            }
        }
    } while (0);
    if (!success) {
        Message message = MessageHelper::NewSharedLogOpFailed(
            SharedLogResultType::DISCARDED);
        FinishLogOp(op, &message);
        return;
    }
    if (primary_node_id == my_node_id()) {
        ReplicateLog(view, op->log_tag, localid, data);
        if (fast_append && op->src_node_id != my_node_id()) {
            Message message = MessageHelper::NewSharedLogOpSucceeded(
                SharedLogResultType::LOCALID);
            message.log_localid = localid;
            FinishLogOp(op, &message);
        }
    } else {
        Message message = MessageHelper::NewSharedLogAppend(op->log_tag, op->id);
        MessageHelper::SetInlineData(&message, data);
        message.hop_times = ++op->hop_times;
        SendMessageToEngine(primary_node_id, &message);
    }
}

void SLogEngine::NewReadAtLogOp(LogOp* op, const log::Fsm::View* view, uint16_t primary_node_id) {
    DCHECK(op_type(op) == LogOpType::kReadAt);
    HVLOG(1) << fmt::format("Read log (seqnum={:#018x}) with primary_node {}",
                            op->log_seqnum, primary_node_id);
    if (view->IsStorageNodeOf(primary_node_id, my_node_id())) {
        HVLOG(1) << fmt::format("Find log (seqnum={:#018x}) locally", op->log_seqnum);
        Message response;
        ReadLogData(op->log_seqnum, op->min_fsm_progress, &response);
        FinishLogOp(op, &response);
        return;
    }
    {
        absl::MutexLock lk(&mu_);
        remote_ops_[op->id] = op;
    }
    Message message = MessageHelper::NewSharedLogReadAt(
        op->log_seqnum, op->min_fsm_progress, op->id);
    message.hop_times = ++op->hop_times;
    SendMessageToEngine(view->PickOneStorageNode(primary_node_id), &message);
}

#if 0
void SLogEngine::NewReadLogOp(LogOp* op) {
    DCHECK(op->log_tag != log::kEmptyLogTag);
    DCHECK(op_type(op) == LogOpType::kReadNext || op_type(op) == LogOpType::kReadPrev);
    int direction = (op_type(op) == LogOpType::kReadNext) ? 1 : -1;
    do {
        uint16_t view_id = log::SeqNumToViewId(op->log_seqnum);
        const log::Fsm::View* view = nullptr;
        uint32_t fsm_progress;
        {
            absl::ReaderMutexLock lk(&mu_);
            fsm_progress = core_.fsm_progress(EngineCore::kStorageProgress);
            DCHECK_GE(fsm_progress, op->min_fsm_progress);
            view = core_.fsm()->view_with_id(view_id);
            if (view == nullptr && direction < 0) {
                view = core_.fsm()->current_view();
                if (view != nullptr) {
                    view_id = view->id();
                    op->log_seqnum = log::BuildSeqNum(view_id + 1, 0) - 1;
                }
            }
        }
        if (view == nullptr) {
            Message message = MessageHelper::NewSharedLogOpFailed(
                SharedLogResultType::EMPTY);
            FinishLogOp(op, &message);
            return;
        }
        uint16_t primary_node_id = view->LogTagToPrimaryNode(op->log_tag);
        if (view->IsStorageNodeOf(primary_node_id, my_node_id())) {
            uint64_t seqnum;
            std::string data;
            bool found = false;
            if (direction > 0) {
                found = storage_->ReadFirst(
                    /* tag=          */ op->log_tag,
                    /* start_seqnum= */ op->log_seqnum,
                    /* end_seqnum=   */ log::BuildSeqNum(view_id + 1, 0),
                    &seqnum, &data);
            } else {
                found = storage_->ReadLast(
                    /* tag=          */ op->log_tag,
                    /* start_seqnum= */ log::BuildSeqNum(view_id, 0),
                    /* end_seqnum=   */ op->log_seqnum + 1,
                    &seqnum, &data);
            }
            if (found) {
                Message message;
                FillReadLogResponse(seqnum, data, fsm_progress, &message);
                FinishLogOp(op, &message);
                return;
            }
        } else {
            uint16_t dst_node_id = view->PickOneStorageNode(primary_node_id);
            Message message = MessageHelper::NewSharedLogRead(
                op->log_tag, op->log_seqnum, direction, fsm_progress,
                /* log_client_data= */ op->id);
            message.hop_times = ++op->hop_times;
            if (op->src_node_id == my_node_id()) {
                {
                    absl::MutexLock lk(&mu_);
                    remote_ops_[op->id] = op;
                }
                SendMessageToEngine(dst_node_id, &message);
            } else {
                ForwardLogOp(op, dst_node_id, &message);
            }
            return;
        }
        if (direction > 0) {
            // Consider the next view
            op->log_seqnum = log::BuildSeqNum(view_id + 1, 0);
        } else {
            if (view_id == 0) {
                Message message = MessageHelper::NewSharedLogOpFailed(
                    SharedLogResultType::EMPTY);
                FinishLogOp(op, &message);
                return;
            }
            // Consider the previous view
            op->log_seqnum = log::BuildSeqNum(view_id, 0) - 1;
        }
    } while (true);
}
#endif

void SLogEngine::ReplicateLog(const log::Fsm::View* view, uint64_t tag,
                              uint64_t localid, std::span<const char> data) {
    HVLOG(1) << fmt::format("Will replicate log (view_id={:#018x}, localid={:#018x}) "
                            "to backup nodes", view->id(), localid);
    Message message = MessageHelper::NewSharedLogReplicate(tag, localid);
    message.src_node_id = my_node_id();
    MessageHelper::SetInlineData(&message, data);
    server::IOWorker* io_worker = server::IOWorker::current();
    DCHECK(io_worker != nullptr);
    SLogMessageHub* hub = DCHECK_NOTNULL(
        io_worker->PickConnection(kSLogMessageHubTypeId))->as_ptr<SLogMessageHub>();
    view->ForEachBackupNode(my_node_id(), [hub, &message] (uint16_t node_id) {
        hub->SendMessage(node_id, message);
    });
}

void SLogEngine::ReadLogData(uint64_t seqnum, uint32_t fsm_progress,
                             protocol::Message* response) {
    LogRecord record;
    absl::ReaderMutexLock lk(&mu_);
    if (core_.ReadLogData(seqnum, &record)) {
        FillReadLogResponse(seqnum, record, fsm_progress, response);
    } else {
        HLOG(WARNING) << fmt::format("Failed to read log with seqnum {:#018x} from log stroage",
                                     seqnum);
        *response = MessageHelper::NewSharedLogOpFailed(SharedLogResultType::DATA_LOST);
    }
}

void SLogEngine::RetryAppendOpIfDoable(LogOp* op) {
    DCHECK(op_type(op) == LogOpType::kAppend);
    DCHECK_EQ(op->src_node_id, my_node_id());
    if (op->src_node_id == my_node_id() && --op->remaining_retries > 0) {
        DCHECK(!op->log_data.empty());
        NewAppendLogOp(op, op->log_data.to_span());
    } else {
        Message response = MessageHelper::NewSharedLogOpFailed(
            SharedLogResultType::DISCARDED);
        FinishLogOp(op, &response);
    }
}

void SLogEngine::RecordLogOpCompletion(LogOp* op) {
    int64_t elapsed_time = GetMonotonicMicroTimestamp() - op->start_timestamp;
    HVLOG(1) << fmt::format("{} op finished: elapsed_time={}, hop={}",
                            kLopOpTypeStr[op_type(op)],
                            elapsed_time, op->hop_times);
}

bool SLogEngine::CheckForFsmProgress(EngineCore::FsmProgressKind kind,
                                     const FuncCallContext* local_ctx,
                                     const protocol::Message& message) {
    uint32_t min_fsm_progress = (local_ctx != nullptr) ? local_ctx->fsm_progress
                                                       : message.log_fsm_progress;
    if (min_fsm_progress == 0) {
        return true;
    }
    uint32_t fsm_progress = known_fsm_progress_[kind].load(std::memory_order_relaxed);
    if (fsm_progress >= min_fsm_progress) {
        return true;
    }
    absl::ReaderMutexLock lk1(&mu_);
    if (core_.fsm_progress(kind) >= min_fsm_progress) {
        return true;
    }
    absl::MutexLock lk2(&pending_requests_mu_);
    PendingRequest* request = pending_request_pool_.Get();
    if (local_ctx != nullptr) {
        request->local = true;
        request->ctx = *local_ctx;
    } else {
        request->local = false;
    }
    memcpy(&request->message, &message, sizeof(protocol::Message));
    pending_requests_[kind].insert(std::make_pair(min_fsm_progress, request));
    return false;
}

void SLogEngine::AdvanceFsmProgress(EngineCore::FsmProgressKind kind,
                                    uint32_t fsm_progress) {
    std::atomic<uint32_t>& target = known_fsm_progress_[kind];
    uint32_t old_progress = target.load(std::memory_order_relaxed);
    while (true) {
        if (old_progress >= fsm_progress) {
            return;
        }
        if (target.compare_exchange_weak(old_progress, fsm_progress,
                                         std::memory_order_release,
                                         std::memory_order_relaxed)) {
            break;
        }
    }
    absl::InlinedVector<PendingRequest*, 8> requests;
    {
        absl::MutexLock lk(&pending_requests_mu_);
        auto iter = pending_requests_[kind].begin();
        while (iter != pending_requests_[kind].end()) {
            if (fsm_progress < iter->first) {
                break;
            }
            requests.push_back(iter->second);
            iter = pending_requests_[kind].erase(iter);
        }
    }
    if (requests.empty()) {
        return;
    }
    for (const PendingRequest* request : requests) {
        if (request->local) {
            HandleLocalRequest(request->ctx, request->message);
        } else {
            HandleRemoteRequest(request->message);
        }
    }
    {
        absl::MutexLock lk(&pending_requests_mu_);
        for (PendingRequest* request : requests) {
            pending_request_pool_.Return(request);
        }
    }
}

void SLogEngine::SendFailedResponse(const protocol::Message& request,
                                    SharedLogResultType result) {
    Message response = MessageHelper::NewSharedLogOpFailed(result);
    response.log_client_data = request.log_client_data;
    engine_->SendFuncWorkerMessage(request.log_client_id, &response);
}

void SLogEngine::SendSequencerMessage(const protocol::SequencerMessage& message,
                                      std::span<const char> payload) {
    server::IOWorker* io_worker = server::IOWorker::current();
    DCHECK(io_worker != nullptr);
    sequencer_config_->ForEachPeer([io_worker, &message, payload]
                                   (const SequencerConfig::Peer* peer) {
        server::ConnectionBase* conn = io_worker->PickConnection(SequencerConnection::type_id(peer->id));
        if (conn != nullptr) {
            conn->as_ptr<SequencerConnection>()->SendMessage(message, payload);
        } else {
            HLOG(ERROR) << fmt::format("No connection for sequencer {} associated with "
                                       "current server::IOWorker", peer->id);
        }
    });
}

void SLogEngine::SendMessageToEngine(uint16_t node_id, protocol::Message* message) {
    SendMessageToEngine(my_node_id(), node_id, message);
}

void SLogEngine::SendMessageToEngine(uint16_t src_node_id, uint16_t dst_node_id,
                                     protocol::Message* message) {
    server::IOWorker* io_worker = server::IOWorker::current();
    DCHECK(io_worker != nullptr);
    SLogMessageHub* hub = DCHECK_NOTNULL(
        io_worker->PickConnection(kSLogMessageHubTypeId))->as_ptr<SLogMessageHub>();
    if (MessageHelper::GetSharedLogOpType(*message) != SharedLogOpType::RESPONSE) {
        // `src_node_id` and `log_result` shares space
        // `src_node_id` is not useful for response message
        message->src_node_id = src_node_id;
    }
    hub->SendMessage(dst_node_id, *message);
}

std::string SLogEngine::GetNodeAddr(uint16_t node_id) {
    absl::ReaderMutexLock lk(&mu_);
    return core_.fsm()->get_addr(node_id);
}

void SLogEngine::DoStateCheck() {
    std::ostringstream stream;
    absl::ReaderMutexLock lk(&mu_);
    int64_t current_timestamp = GetMonotonicMicroTimestamp();
    core_.DoStateCheck(stream);
    if (!append_ops_.empty()) {
        stream << fmt::format("There are {} local log appends:\n",
                              append_ops_.size());
        int counter = 0;
        for (const auto& entry : append_ops_) {
            uint64_t localid = entry.first;
            const LogOp* op = entry.second;
            counter++;
            stream << fmt::format("--[{}] LocalId={:#018x} Tag={} ElapsedTime={}us",
                                  counter, localid, op->log_tag,
                                  current_timestamp - op->start_timestamp);
            if (op->src_node_id == my_node_id()) {
                stream << " SrcNode=myself";
            } else {
                stream << " SrcNode=" << op->src_node_id;
            }
            stream << "\n";
            if (counter >= 32) {
                stream << "...more...\n";
                break;
            }
        }
    }
    if (!remote_ops_.empty()) {
        stream << fmt::format("There are {} LogOp running remotely:\n",
                              remote_ops_.size());
        int counter = 0;
        for (const auto& entry : remote_ops_) {
            counter++;
            const LogOp* op = entry.second;
            stream << fmt::format("--[{}] {} ElapsedTime={}us",
                                  counter, kLopOpTypeStr[op_type(op)],
                                  current_timestamp - op->start_timestamp);
            switch (op_type(op)) {
            case kReadPrev:
            case kReadNext:
                stream << " Tag=" << op->log_tag;
                ABSL_FALLTHROUGH_INTENDED;
            case kReadAt:
                stream << fmt::format(" SeqNum={:#018x}", op->log_seqnum);
                ABSL_FALLTHROUGH_INTENDED;
            default:
                break;
            }
            stream << "\n";
            if (counter >= 32) {
                stream << "...more...\n";
                break;
            }
        }
    }
    std::string output = stream.str();
    if (output.empty()) {
        return;
    }
    LOG(INFO) << "\n"
              << "==================BEGIN SLOG STATE CHECK==================\n"
              << output
              << "===================END SLOG STATE CHECK===================";
}

}  // namespace engine
}  // namespace faas
