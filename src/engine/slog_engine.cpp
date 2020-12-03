#include "engine/slog_engine.h"

#include "common/time.h"
#include "log/common.h"
#include "engine/constants.h"
#include "engine/flags.h"
#include "engine/sequencer_connection.h"
#include "engine/engine.h"

#define log_header_ "SLogEngine: "

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
      next_op_id_(1) {
    core_.SetLogPersistedCallback(
        absl::bind_front(&SLogEngine::LogPersisted, this));
    core_.SetLogDiscardedCallback(
        absl::bind_front(&SLogEngine::LogDiscarded, this));
    core_.SetScheduleLocalCutCallback(
        absl::bind_front(&SLogEngine::ScheduleLocalCut, this));
    std::string storage_backend = absl::GetFlag(FLAGS_slog_storage_backend);
    if (storage_backend == "inmem") {
        storage_.reset(new log::InMemoryStorage());
    } else if (storage_backend == "rocksdb") {
        std::string db_path = absl::GetFlag(FLAGS_slog_storage_datadir);
        CHECK(!db_path.empty()) << "Empty slog_storage_datadir";
        storage_.reset(new log::RocksDBStorage(db_path));
    } else {
        HLOG(FATAL) << "Unknown storage backend: " << storage_backend;
    }
    SetupTimers();
}

SLogEngine::~SLogEngine() {}

uint16_t SLogEngine::my_node_id() const {
    return engine_->node_id();
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
            kSLogEngineTimerTypeId, io_worker,
            absl::bind_front(&SLogEngine::LocalCutTimerTriggered, this));
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

void SLogEngine::OnSequencerMessage(const SequencerMessage& message,
                                    std::span<const char> payload) {
    if (SequencerMessageHelper::IsFsmRecords(message)) {
        log::FsmRecordsMsgProto message_proto;
        if (!message_proto.ParseFromArray(payload.data(), payload.size())) {
            HLOG(ERROR) << "Failed to parse sequencer message!";
            return;
        }
        absl::MutexLock lk(&mu_);
        core_.OnNewFsmRecordsMessage(message_proto);
    } else {
        HLOG(ERROR) << fmt::format("Unknown message type: {}!", message.message_type);
    }
}

SLogEngine::LogOp* SLogEngine::AllocLogOp(LogOpType type, uint16_t client_id,
                                          uint64_t client_data) {
    LogOp* op = log_op_pool_.Get();
    op->id = (next_op_id_.fetch_add(1) << 8) + uint16_t{type};
    op->client_id = client_id;
    op->src_node_id = my_node_id();
    op->client_data = client_data;
    op->log_tag = protocol::kInvalidLogTag;
    op->log_seqnum = protocol::kInvalidLogSeqNum;
    op->log_data.clear();
    op->remaining_retries = kMaxRetires;
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
    } else if (op_type == SharedLogOpType::CHECK_TAIL) {
        HandleLocalCheckTail(message);
    } else if (op_type == SharedLogOpType::READ_NEXT) {
        HandleLocalReadNext(message);
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
    LogOp* op = AllocLogOp(LogOpType::kAppend, /* client_id= */ 0, message.log_client_data);
    op->log_tag = message.log_tag;
    op->src_node_id = message.src_node_id;
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
    SendMessageToEngine(message.src_node_id, &response);
}

void SLogEngine::HandleRemoteRead(const protocol::Message& message) {
    SharedLogOpType msg_type = MessageHelper::GetSharedLogOpType(message);
    DCHECK(msg_type == SharedLogOpType::READ_NEXT || msg_type == SharedLogOpType::READ_PREV);
    DCHECK(message.log_tag != log::kDefaultLogTag);
    LogOp* op = nullptr;
    LogOpType type = (msg_type == SharedLogOpType::READ_NEXT) ? LogOpType::kReadNext
                                                              : LogOpType::kReadPrev;
    if (message.src_node_id == my_node_id()) {
        // Route back to myself
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
    } else {
        op = AllocLogOp(type, /* client_id= */ 0, message.log_client_data);
        op->log_tag = message.log_tag;
        op->log_seqnum = message.log_seqnum;
        op->src_node_id = message.src_node_id;
    }
    NewReadLogOp(op);
}

void SLogEngine::HandleLocalAppend(const Message& message) {
    std::span<const char> data = MessageHelper::GetInlineData(message);
    if (data.empty()) {
        SendFailedResponse(message, SharedLogResultType::BAD_ARGS);
        return;
    }
    LogOp* op = AllocLogOp(LogOpType::kAppend, message.log_client_id, message.log_client_data);
    op->log_tag = message.log_tag;
    NewAppendLogOp(op, data);
}

void SLogEngine::HandleLocalReadNext(const protocol::Message& message) {
    if (message.log_tag == log::kDefaultLogTag) {
        const log::Fsm::View* view;
        uint64_t seqnum;
        uint16_t primary_node_id;
        bool success = false;
        {
            absl::ReaderMutexLock lk(&mu_);
            success = core_.fsm()->FindNextSeqnum(
                message.log_seqnum, &seqnum, &view, &primary_node_id);
        }
        if (!success) {
            SendFailedResponse(message, SharedLogResultType::EMPTY);
            return;
        }
        LogOp* op = AllocLogOp(LogOpType::kReadAt,
                               message.log_client_id, message.log_client_data);
        op->log_seqnum = seqnum;
        NewReadAtLogOp(op, view, primary_node_id);
    } else {
        LogOp* op = AllocLogOp(LogOpType::kReadNext,
                               message.log_client_id, message.log_client_data);
        op->log_seqnum = message.log_seqnum;
        op->log_tag = message.log_tag;
        NewReadLogOp(op);
    }
}

void SLogEngine::HandleLocalCheckTail(const protocol::Message& message) {
    if (message.log_tag == log::kDefaultLogTag) {
        const log::Fsm::View* view;
        uint64_t seqnum;
        uint16_t primary_node_id;
        bool success = false;
        {
            absl::ReaderMutexLock lk(&mu_);
            success = core_.fsm()->CheckTail(&seqnum, &view, &primary_node_id);
        }
        if (!success) {
            SendFailedResponse(message, SharedLogResultType::EMPTY);
            return;
        }
        LogOp* op = AllocLogOp(LogOpType::kReadAt,
                               message.log_client_id, message.log_client_data);
        op->log_tag = message.log_tag;
        op->log_seqnum = seqnum;
        NewReadAtLogOp(op, view, primary_node_id);
    } else {
        const log::Fsm::View* view;
        {
            absl::ReaderMutexLock lk(&mu_);
            view = core_.fsm()->current_view();
        }
        if (view == nullptr) {
            SendFailedResponse(message, SharedLogResultType::EMPTY);
            return;
        }
        LogOp* op = AllocLogOp(LogOpType::kReadPrev,
                               message.log_client_id, message.log_client_data);
        op->log_seqnum = log::BuildSeqNum(view->id() + 1, 0) - 1;
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
    if (result == SharedLogResultType::DISCARDED && --op->remaining_retries > 0) {
        std::span<const char> data(op->log_data.data(), op->log_data.size());
        NewAppendLogOp(op, data);
    } else {
        Message response = message;
        FinishLogOp(op, &response);
    }
}

void SLogEngine::RemoteReadAtFinished(const protocol::Message& message, LogOp* op) {
    Message response = message;
    response.log_seqnum = op->log_seqnum;
    FinishLogOp(op, &response);
}

void SLogEngine::RemoteReadFinished(const protocol::Message& message, LogOp* op) {
    Message response = message;
    FinishLogOp(op, &response);
}

void SLogEngine::LogPersisted(std::unique_ptr<log::LogEntry> log_entry) {
    mu_.AssertHeld();
    uint64_t localid = log_entry->localid;
    uint64_t seqnum = log_entry->seqnum;
    storage_->Add(std::move(log_entry));
    if (log::LocalIdToNodeId(localid) == my_node_id()) {
        LogOp* op = GrabLogOp(append_ops_, localid);
        if (op == nullptr) {
            return;
        }
        HVLOG(1) << fmt::format("Log (localid {}) replicated with seqnum {}",
                                localid, seqnum);
        Message response = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogResultType::APPEND_OK, seqnum);
        FinishLogOp(op, &response);
    }
}

void SLogEngine::LogDiscarded(std::unique_ptr<log::LogEntry> log_entry) {
    mu_.AssertHeld();
    if (log::LocalIdToNodeId(log_entry->localid) == my_node_id()) {
        LogOp* op = GrabLogOp(append_ops_, log_entry->localid);
        if (op == nullptr) {
            return;
        }
        HLOG(WARNING) << fmt::format("Log with localid {} discarded", log_entry->localid);
        if (op->src_node_id == my_node_id() && --op->remaining_retries > 0) {
            std::span<const char> data(log_entry->data.data(), log_entry->data.size());
            // This is buggy, as NewAppendLogOp will try to acquire mu_
            // TODO: Fix it
            NewAppendLogOp(op, data);
        } else {
            Message response = MessageHelper::NewSharedLogOpFailed(
                SharedLogResultType::DISCARDED);
            FinishLogOp(op, &response);
        }
    }
}

void SLogEngine::FinishLogOp(LogOp* op, protocol::Message* response) {
    if (response != nullptr) {
        response->log_client_data = op->client_data;
        if (op->src_node_id == my_node_id()) {
            engine_->SendFuncWorkerMessage(op->client_id, response);
        } else {
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
    const log::Fsm::View* view;
    uint16_t primary_node_id;
    uint64_t localid;
    bool success = false;
    do {
        absl::MutexLock lk(&mu_);
        success = core_.LogTagToPrimaryNode(op->log_tag, &primary_node_id);
        if (!success) {
            break;
        }
        if (primary_node_id == my_node_id()) {
            success = core_.StoreLogAsPrimaryNode(op->log_tag, data, &localid);
            if (success) {
                view = core_.fsm()->view_with_id(log::LocalIdToViewId(localid));
                DCHECK(view != nullptr);
                append_ops_[localid] = op;
            }
        } else {
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
    } else {
        if (op->log_data.empty()) {
            op->log_data.assign(data.data(), data.size());
        }
        Message message = MessageHelper::NewSharedLogAppend(op->log_tag, op->id);
        MessageHelper::SetInlineData(&message, data);
        SendMessageToEngine(primary_node_id, &message);
    }
}

void SLogEngine::NewReadAtLogOp(LogOp* op, const log::Fsm::View* view, uint16_t primary_node_id) {
    DCHECK(op_type(op) == LogOpType::kReadAt);
    if (view->IsStorageNodeOf(primary_node_id, my_node_id())) {
        HVLOG(1) << fmt::format("Find log (seqnum={}) locally", op->log_seqnum);
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
    SendMessageToEngine(view->PickOneStorageNode(primary_node_id), &message);
}

void SLogEngine::NewReadLogOp(LogOp* op) {
    DCHECK(op->log_tag != log::kDefaultLogTag);
    DCHECK(op_type(op) == LogOpType::kReadNext || op_type(op) == LogOpType::kReadPrev);
    do {
        uint16_t view_id = log::SeqNumToViewId(op->log_seqnum);
        const log::Fsm::View* view = nullptr;
        {
            absl::ReaderMutexLock lk(&mu_);
            view = core_.fsm()->view_with_id(view_id);
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
            if (op_type(op) == LogOpType::kReadNext) {
                found = storage_->ReadFirst(op->log_tag,
                                            /* start_seqnum= */ op->log_seqnum,
                                            /* end_seqnum= */ log::BuildSeqNum(view_id + 1, 0),
                                            &seqnum, &data);
            } else {
                found = storage_->ReadLast(op->log_tag,
                                           /* start_seqnum= */ log::BuildSeqNum(view_id, 0),
                                           /* end_seqnum= */ op->log_seqnum + 1,
                                           &seqnum, &data);
            }
            if (found) {
                Message message;
                FillReadLogResponse(seqnum, data, &message);
                FinishLogOp(op, &message);
                return;
            }
        } else {
            uint16_t dst_node_id = view->PickOneStorageNode(primary_node_id);
            Message message = MessageHelper::NewSharedLogRead(
                op->log_tag, op->log_seqnum,
                /* next= */ op_type(op) == LogOpType::kReadNext,
                /* log_client_data= */ op->id);
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
        if (op_type(op) == LogOpType::kReadNext) {
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

void SLogEngine::ReplicateLog(const log::Fsm::View* view, int32_t tag,
                              uint64_t localid, std::span<const char> data) {
    HVLOG(1) << fmt::format("Will replicate log (view_id={}, localid={}) to backup nodes",
                            view->id(), localid);
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
        *response = MessageHelper::NewSharedLogOpFailed(SharedLogResultType::DATA_LOST);
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
    message->src_node_id = src_node_id;
    hub->SendMessage(dst_node_id, *message);
}

void SLogEngine::ScheduleLocalCut(int duration_us) {
    mu_.AssertHeld();
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* timer = io_worker->PickConnection(kSLogEngineTimerTypeId);
    DCHECK(timer != nullptr);
    timer->as_ptr<Timer>()->TriggerIn(duration_us);
}

std::string SLogEngine::GetNodeAddr(uint16_t node_id) {
    absl::ReaderMutexLock lk(&mu_);
    return core_.fsm()->get_addr(node_id);
}

}  // namespace engine
}  // namespace faas
