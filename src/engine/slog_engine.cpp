#include "engine/slog_engine.h"

#include "common/time.h"
#include "log/common.h"
#include "engine/constants.h"
#include "engine/flags.h"
#include "engine/sequencer_connection.h"
#include "engine/engine.h"

#define log_header_ "SLogEngine: "

ABSL_FLAG(bool, slog_engine_fast_append, false, "");

namespace faas {
namespace engine {

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
      statecheck_timer_(nullptr) {
    core_.SetLogPersistedCallback(
        absl::bind_front(&SLogEngine::LogPersisted, this));
    core_.SetLogDiscardedCallback(
        absl::bind_front(&SLogEngine::LogDiscarded, this));
    core_.SetScheduleLocalCutCallback(
        absl::bind_front(&SLogEngine::ScheduleLocalCut, this));
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
inline void FillReadLogResponse(uint64_t seqnum, const std::string& data,
                                protocol::Message* response) {
    if (data.size() > MESSAGE_INLINE_DATA_SIZE) {
        LOG(FATAL) << "Log data too long to fit into one message, "
                      "this should not happend given current implementation";
    }
    *response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::READ_OK, seqnum);
    MessageHelper::SetInlineData(response, data);
}
}

void SLogEngine::SetupTimers() {
    for (IOWorker* io_worker : engine_->io_workers_) {
        engine_->CreateTimer(
            kSLogLocalCutTimerTypeId, io_worker,
            absl::bind_front(&SLogEngine::LocalCutTimerTriggered, this));
    }
    if (absl::GetFlag(FLAGS_slog_enable_statecheck)) {
        int interval_us = absl::GetFlag(FLAGS_slog_statecheck_interval_sec) * 1000000;
        statecheck_timer_ = engine_->CreateTimer(
            kSLogStateCheckTimerTypeId, engine_->io_workers_.front(),
            absl::bind_front(&SLogEngine::StateCheckTimerTriggered, this),
            /* initial_duration_us= */ interval_us);
    }
}

void SLogEngine::LocalCutTimerTriggered() {
    mu_.AssertNotHeld();
    HVLOG(1) << "LocalCutTimerTriggered";
    log::LocalCutMsgProto message;
    {
        absl::MutexLock lk(&mu_);
        core_.BuildLocalCutMessage(&message);
    }
    std::string serialized_message;
    message.SerializeToString(&serialized_message);
    std::span<const char> data(serialized_message.data(), serialized_message.length());
    SendSequencerMessage(SequencerMessageHelper::NewLocalCut(data), data);
}

void SLogEngine::StateCheckTimerTriggered() {
    mu_.AssertNotHeld();
    HVLOG(1) << "StateCheckTimerTriggered";
    DoStateCheck();
    int interval_us = absl::GetFlag(FLAGS_slog_statecheck_interval_sec) * 1000000;
    statecheck_timer_->TriggerIn(interval_us);
}

void SLogEngine::OnSequencerMessage(const SequencerMessage& message,
                                    std::span<const char> payload) {
    if (SequencerMessageHelper::IsFsmRecords(message)) {
        log::FsmRecordsMsgProto message_proto;
        if (!message_proto.ParseFromArray(payload.data(), payload.size())) {
            HLOG(ERROR) << "Failed to parse sequencer message!";
            return;
        }
        absl::InlinedVector<CompletedLogEntry, 8> completed_log_entries;
        uint32_t fsm_progress;
        {
            absl::MutexLock lk(&mu_);
            core_.OnNewFsmRecordsMessage(message_proto);
            fsm_progress = core_.fsm_progress();
            completed_log_entries_.swap(completed_log_entries);
        }
        for (CompletedLogEntry& entry : completed_log_entries) {
            LogEntryCompleted(std::move(entry), fsm_progress);
        }
    } else {
        HLOG(ERROR) << fmt::format("Unknown message type: {}!", message.message_type);
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
    op->log_data.clear();
    op->func_call = protocol::kInvalidFuncCall;
    op->remaining_retries = kMaxRetires;
    op->start_timestamp = GetMonotonicMicroTimestamp();
    op->hop_times = 0;
    return op;
}

void SLogEngine::OnMessageFromOtherEngine(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        HandleRemoteAppend(message);
    } else if (op_type == SharedLogOpType::REPLICATE) {
        HandleRemoteReplicate(message);
    } else if (op_type == SharedLogOpType::READ_AT) {
        HandleRemoteReadAt(message);
    } else if (op_type == SharedLogOpType::READ_NEXT
               || op_type == SharedLogOpType::READ_PREV) {
        HandleRemoteRead(message);
    } else if (op_type == SharedLogOpType::RESPONSE) {
        RemoteOpFinished(message);
    } else {
        HLOG(FATAL) << "Unknown SharedLogOpType from remote message: "
                    << static_cast<uint16_t>(op_type);
    }
}

void SLogEngine::OnMessageFromFuncWorker(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        HandleLocalAppend(message);
    } else if (op_type == SharedLogOpType::READ_NEXT) {
        HandleLocalRead(message, /* direction= */ 1);
    } else if (op_type == SharedLogOpType::READ_PREV) {
        HandleLocalRead(message, /* direction= */ -1);
    } else if (op_type == SharedLogOpType::TRIM) {
        // TODO
        NOT_IMPLEMENTED();
    } else {
        HLOG(FATAL) << "Unknown SharedLogOpType from local message: "
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

void SLogEngine::HandleRemoteReadAt(const protocol::Message& message) {
    DCHECK(MessageHelper::GetSharedLogOpType(message) == SharedLogOpType::READ_AT);
    DCHECK(message.src_node_id != my_node_id());
    Message response;
    ReadLogFromStorage(message.log_seqnum, &response);
    response.log_client_data = message.log_client_data;
    response.hop_times = message.hop_times + 1;
    SendMessageToEngine(message.src_node_id, &response);
}

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

void SLogEngine::HandleLocalAppend(const Message& message) {
    std::span<const char> data = MessageHelper::GetInlineData(message);
    if (data.empty()) {
        SendFailedResponse(message, SharedLogResultType::BAD_ARGS);
        return;
    }
    protocol::FuncCall func_call = MessageHelper::GetFuncCall(message);
    FuncCallContext ctx = GetFuncContext(func_call);
    LogOp* op = AllocLogOp(LogOpType::kAppend, ctx.log_space, ctx.fsm_progress,
                           message.log_client_id, message.log_client_data);
    op->func_call = func_call;
    op->log_tag = message.log_tag;
    if (op->log_tag == log::kEmptyLogTag) {
        HVLOG(1) << "Local append with default tag";
    } else {
        HVLOG(1) << fmt::format("Local append with tag {}", op->log_tag);
    }
    op->log_data.assign(data.data(), data.size());
    NewAppendLogOp(op, data);
}

void SLogEngine::HandleLocalRead(const protocol::Message& message, int direction) {
    DCHECK(direction != 0);
    protocol::FuncCall func_call = MessageHelper::GetFuncCall(message);
    FuncCallContext ctx = GetFuncContext(func_call);
    if (message.log_tag == log::kEmptyLogTag) {
        HVLOG(1) << fmt::format("Local read[{}] with default tag", direction);
        const log::Fsm::View* view;
        uint64_t seqnum;
        uint16_t primary_node_id;
        uint32_t fsm_progress;
        bool success = false;
        {
            absl::ReaderMutexLock lk(&mu_);
            fsm_progress = core_.fsm_progress();
            if (direction > 0) {
                success = core_.fsm()->FindNextSeqnum(
                    message.log_seqnum, &seqnum, &view, &primary_node_id);
            } else {
                success = core_.fsm()->FindPrevSeqnum(
                    message.log_seqnum, &seqnum, &view, &primary_node_id);
            }
        }
        if (!success) {
            SendFailedResponse(message, SharedLogResultType::EMPTY);
            return;
        }
        LogOp* op = AllocLogOp(LogOpType::kReadAt, ctx.log_space, fsm_progress,
                               message.log_client_id, message.log_client_data);
        op->func_call = func_call;
        op->log_seqnum = seqnum;
        NewReadAtLogOp(op, view, primary_node_id);
    } else {
        HVLOG(1) << fmt::format("Local read[{}] with tag {}", direction, message.log_tag);
        LogOp* op = AllocLogOp((direction > 0) ? LogOpType::kReadNext : LogOpType::kReadPrev,
                               ctx.log_space, ctx.fsm_progress,
                               message.log_client_id, message.log_client_data);
        op->func_call = func_call;
        op->log_seqnum = message.log_seqnum;
        op->log_tag = message.log_tag;
        NewReadLogOp(op);
    }
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
            fsm_progress = core_.fsm_progress();
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
    LogOp* op = GrabLogOp(append_ops_, localid);
    completed_log_entries_.push_back({
        .localid = localid,
        .seqnum = seqnum,
        .append_op = op
    });
}

void SLogEngine::LogDiscarded(uint64_t localid) {
    mu_.AssertHeld();
    LogOp* op = GrabLogOp(append_ops_, localid);
    completed_log_entries_.push_back({
        .localid = localid,
        .seqnum = protocol::kInvalidLogSeqNum,
        .append_op = op
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
        success = core_.LogTagToPrimaryNode(op->log_tag, &primary_node_id);
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
        ReadLogFromStorage(op->log_seqnum, &response);
        FinishLogOp(op, &response);
        return;
    }
    {
        absl::MutexLock lk(&mu_);
        remote_ops_[op->id] = op;
    }
    Message message = MessageHelper::NewSharedLogReadAt(op->log_seqnum, op->id);
    message.hop_times = ++op->hop_times;
    SendMessageToEngine(view->PickOneStorageNode(primary_node_id), &message);
}

void SLogEngine::NewReadLogOp(LogOp* op) {
    DCHECK(op->log_tag != log::kEmptyLogTag);
    DCHECK(op_type(op) == LogOpType::kReadNext || op_type(op) == LogOpType::kReadPrev);
    int direction = (op_type(op) == LogOpType::kReadNext) ? 1 : -1;
    do {
        uint16_t view_id = log::SeqNumToViewId(op->log_seqnum);
        const log::Fsm::View* view = nullptr;
        {
            absl::ReaderMutexLock lk(&mu_);
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
        // if (view->IsStorageNodeOf(primary_node_id, my_node_id())) {
        if (primary_node_id == my_node_id()) {
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
                FillReadLogResponse(seqnum, data, &message);
                FinishLogOp(op, &message);
                return;
            }
        } else {
            // uint16_t dst_node_id = view->PickOneStorageNode(primary_node_id);
            uint16_t dst_node_id = primary_node_id;
            Message message = MessageHelper::NewSharedLogRead(
                op->log_tag, op->log_seqnum, direction,
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

void SLogEngine::ReplicateLog(const log::Fsm::View* view, uint64_t tag,
                              uint64_t localid, std::span<const char> data) {
    HVLOG(1) << fmt::format("Will replicate log (view_id={:#018x}, localid={:#018x}) "
                            "to backup nodes", view->id(), localid);
    Message message = MessageHelper::NewSharedLogReplicate(tag, localid);
    message.src_node_id = my_node_id();
    MessageHelper::SetInlineData(&message, data);
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    SLogMessageHub* hub = DCHECK_NOTNULL(
        io_worker->PickConnection(kSLogMessageHubTypeId))->as_ptr<SLogMessageHub>();
    view->ForEachBackupNode(my_node_id(), [hub, &message] (uint16_t node_id) {
        hub->SendMessage(node_id, message);
    });
}

void SLogEngine::ReadLogFromStorage(uint64_t seqnum, protocol::Message* response) {
    std::string data;
    if (storage_->Read(seqnum, &data)) {
        FillReadLogResponse(seqnum, data, response);
    } else {
        HLOG(WARNING) << fmt::format("Failed to read log with seqnum {:#018x} from log stroage",
                                     seqnum);
        *response = MessageHelper::NewSharedLogOpFailed(SharedLogResultType::DATA_LOST);
    }
}

void SLogEngine::LogEntryCompleted(CompletedLogEntry entry, uint32_t fsm_progress) {
    if (entry.seqnum != protocol::kInvalidLogSeqNum) {
        uint64_t seqnum = entry.seqnum;
        // if (entry.log_entry->data.size() > 0) {
        //     storage_->Add(std::move(entry.log_entry), fsm_progress);
        // }
        LogOp* op = entry.append_op;
        if (op == nullptr) {
            return;
        }
        Message response = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogResultType::APPEND_OK, seqnum);
        response.log_fsm_progress = fsm_progress;
        FinishLogOp(op, &response);
    } else {
        HLOG(WARNING) << fmt::format("Log with localid {:#018x} discarded",
                                     entry.localid);
        LogOp* op = entry.append_op;
        if (op == nullptr) {
            return;
        }
        RetryAppendOpIfDoable(op);
    }
}

void SLogEngine::RetryAppendOpIfDoable(LogOp* op) {
    DCHECK(op_type(op) == LogOpType::kAppend);
    DCHECK_EQ(op->src_node_id, my_node_id());
    if (op->src_node_id == my_node_id() && --op->remaining_retries > 0) {
        std::span<const char> data(op->log_data.data(), op->log_data.size());
        DCHECK(data.size() > 0);
        NewAppendLogOp(op, data);
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

void SLogEngine::SendFailedResponse(const protocol::Message& request,
                                    SharedLogResultType result) {
    Message response = MessageHelper::NewSharedLogOpFailed(result);
    response.log_client_data = request.log_client_data;
    engine_->SendFuncWorkerMessage(request.log_client_id, &response);
}

void SLogEngine::SendSequencerMessage(const protocol::SequencerMessage& message,
                                      std::span<const char> payload) {
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    sequencer_config_->ForEachPeer([io_worker, &message, payload]
                                   (const SequencerConfig::Peer* peer) {
        ConnectionBase* conn = io_worker->PickConnection(SequencerConnection::type_id(peer->id));
        if (conn != nullptr) {
            conn->as_ptr<SequencerConnection>()->SendMessage(message, payload);
        } else {
            HLOG(ERROR) << fmt::format("No connection for sequencer {} associated with "
                                       "current IOWorker", peer->id);
        }
    });
}

void SLogEngine::SendMessageToEngine(uint16_t node_id, protocol::Message* message) {
    SendMessageToEngine(my_node_id(), node_id, message);
}

void SLogEngine::SendMessageToEngine(uint16_t src_node_id, uint16_t dst_node_id,
                                     protocol::Message* message) {
    IOWorker* io_worker = IOWorker::current();
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

void SLogEngine::ScheduleLocalCut(int duration_us) {
    mu_.AssertHeld();
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* timer = io_worker->PickConnection(kSLogLocalCutTimerTypeId);
    DCHECK(timer != nullptr);
    timer->as_ptr<Timer>()->TriggerIn(duration_us);
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
