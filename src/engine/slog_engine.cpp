#include "engine/slog_engine.h"

#include "common/time.h"
#include "log/common.h"
#include "engine/constants.h"
#include "engine/sequencer_connection.h"
#include "engine/engine.h"

#define log_header_ "SLogEngine: "

namespace faas {
namespace engine {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogOpType;
using protocol::SequencerMessage;
using protocol::SequencerMessageHelper;

SLogEngine::SLogEngine(Engine* engine)
    : engine_(engine),
      sequencer_config_(&engine->sequencer_config_),
      core_(engine->node_id()),
      storage_(new log::InMemoryStorage()) {
    core_.SetLogPersistedCallback(
        absl::bind_front(&SLogEngine::LogPersisted, this));
    core_.SetLogDiscardedCallback(
        absl::bind_front(&SLogEngine::LogDiscarded, this));
    core_.SetScheduleLocalCutCallback(
        absl::bind_front(&SLogEngine::ScheduleLocalCut, this));
    SetupTimers();
}

SLogEngine::~SLogEngine() {}

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
        core_.NewFsmRecordsMessage(message_proto);
    } else {
        HLOG(ERROR) << fmt::format("Unknown message type: {}!", message.message_type);
    }
}

SLogEngine::LogOp* SLogEngine::AllocLogOp(LogOpType type, uint16_t client_id,
                                          uint64_t client_data) {
    LogOp* op = log_op_pool_.Get();
    op->id = (next_op_id_.fetch_add(1) << 8) + uint16_t{type};
    op->client_id = client_id;
    op->client_data = client_data;
    op->log_tag = protocol::kDefaultLogTag;
    op->log_seqnum = protocol::kInvalidLogSeqNum;
    return op;
}

SLogEngine::LogOp* SLogEngine::GrabAppendLogOp(uint64_t localid) {
    if (!append_ops_.contains(localid)) {
        HLOG(WARNING) << fmt::format("Cannot find ongoing append log (localid {})", localid);
        return nullptr;
    }
    LogOp* op = append_ops_[localid];
    append_ops_.erase(localid);
    return op;
}

SLogEngine::LogOp* SLogEngine::GrabReadLogOp(uint64_t op_id) {
    if (!read_ops_.contains(op_id)) {
        HLOG(WARNING) << "Cannot find ongoing read op";
        return nullptr;
    }
    LogOp* op = read_ops_[op_id];
    read_ops_.erase(op_id);
    return op;
}

void SLogEngine::OnMessageFromOtherEngine(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    if (message.log_tag != protocol::kDefaultLogTag) {
        HLOG(ERROR) << "Cannot handle non-zero log tag at the moment";
        Message response = MessageHelper::NewSharedLogOpFailed(
            SharedLogOpType::BAD_ARGS, message.log_client_data);
        engine_->SendFuncWorkerMessage(message.log_client_id, &response);
        return;
    }
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        HandleRemoteAppend(message);
    } else if (op_type == SharedLogOpType::READ_AT) {
        HandleRemoteReadAt(message);
    } else if (op_type == SharedLogOpType::READ_OK
                 || op_type == SharedLogOpType::DATA_LOST) {
        ReadAtFinished(message);
    } else {
        HLOG(ERROR) << "Unknown SharedLogOpType";
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
    } else {
        HLOG(ERROR) << "Unknown SharedLogOpType";
    }
}

void SLogEngine::HandleRemoteAppend(const Message& message) {
    absl::MutexLock lk(&mu_);
    core_.NewRemoteLog(message.log_localid, message.log_tag,
                       MessageHelper::GetInlineData(message));
}

void SLogEngine::HandleRemoteReadAt(const protocol::Message& message) {
    Message response;
    ReadLogFromStorage(message.log_seqnum, message.log_client_data, &response);
    SendMessageToEngine(message.log_node_id, response);
}

void SLogEngine::HandleLocalAppend(const Message& message) {
    LogOp* op = AllocLogOp(LogOpType::kAppend, message.log_client_id, message.log_client_data);
    op->log_tag = message.log_tag;
    std::span<const char> data = MessageHelper::GetInlineData(message);
    const log::Fsm::View* view;
    uint64_t localid;
    bool success = false;
    {
        absl::MutexLock lk(&mu_);
        success = core_.NewLocalLog(message.log_tag, data, &view, &localid);
        if (success) {
            append_ops_[localid] = op;
        }
    }
    if (!success) {
        HLOG(ERROR) << "NewLocalLog failed";
        log_op_pool_.Return(op);
        Message response = MessageHelper::NewSharedLogOpFailed(
            SharedLogOpType::DISCARDED, message.log_client_data);
        engine_->SendFuncWorkerMessage(message.log_client_id, &response);
        return;
    }

    // Replicate new log to backup nodes
    HVLOG(1) << fmt::format("Will replicate new log (view_id={}, localid={}) to backup nodes",
                            view->id(), localid);
    Message message_copy = message;
    message_copy.log_localid = localid;
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    SLogMessageHub* hub = DCHECK_NOTNULL(
        io_worker->PickConnection(kSLogMessageHubTypeId))->as_ptr<SLogMessageHub>();
    view->ForEachBackupNode(engine_->node_id(), [hub, &message_copy] (uint16_t node_id) {
        hub->SendMessage(node_id, message_copy);
    });
}

void SLogEngine::HandleLocalReadNext(const protocol::Message& message) {
    const log::Fsm::View* view;
    uint64_t seqnum;
    uint16_t primary_node_id;
    bool success = false;
    {
        absl::ReaderMutexLock lk(&mu_);
        success = core_.fsm()->FindNextSeqnum(
            message.log_start_seqnum, &seqnum, &view, &primary_node_id);
    }

    if (success && message.log_end_seqnum <= seqnum) {
        success = false;
    }

    if (!success) {
        Message response = MessageHelper::NewSharedLogOpFailed(
            SharedLogOpType::EMPTY, message.log_client_data);
        engine_->SendFuncWorkerMessage(message.log_client_id, &response);
        return;
    }

    ReadLog(message.log_client_id, message.log_client_data, message.log_tag,
            seqnum, view, primary_node_id);
}

void SLogEngine::HandleLocalCheckTail(const protocol::Message& message) {
    const log::Fsm::View* view;
    uint64_t seqnum;
    uint16_t primary_node_id;
    bool success = false;
    {
        absl::ReaderMutexLock lk(&mu_);
        success = core_.fsm()->CheckTail(&seqnum, &view, &primary_node_id);
    }

    if (!success) {
        Message response = MessageHelper::NewSharedLogOpFailed(
            SharedLogOpType::EMPTY, message.log_client_data);
        engine_->SendFuncWorkerMessage(message.log_client_id, &response);
        return;
    }

    ReadLog(message.log_client_id, message.log_client_data, message.log_tag,
            seqnum, view, primary_node_id);
}

void SLogEngine::ReadLog(uint16_t client_id, uint64_t client_data,
                         uint32_t log_tag, uint64_t log_seqnum,
                         const log::Fsm::View* view, uint16_t primary_node_id) {
    if (view->IsStorageNodeOf(primary_node_id, engine_->node_id())) {
        HVLOG(1) << fmt::format("Find log (seqnum={}) locally", log_seqnum);
        Message response;
        ReadLogFromStorage(log_seqnum, client_data, &response);
        engine_->SendFuncWorkerMessage(client_id, &response);
        return;
    }
    LogOp* op = AllocLogOp(LogOpType::kRead, client_id, client_data);
    op->log_tag = log_tag;
    op->log_seqnum = log_seqnum;
    {
        absl::MutexLock lk(&mu_);
        read_ops_[op->id] = op;
    }
    Message message = MessageHelper::NewSharedLogReadAt(engine_->node_id(), log_seqnum);
    message.log_client_data = op->id;
    SendMessageToEngine(view->PickOneStorageNode(primary_node_id), message);
}

void SLogEngine::ReadAtFinished(const protocol::Message& message) {
    LogOp* op = nullptr;
    {
        absl::MutexLock lk(&mu_);
        op = GrabReadLogOp(message.log_client_data);
    }
    if (op == nullptr) {
        return;
    }
    Message response = message;
    response.log_seqnum = op->log_seqnum;
    response.log_client_data = op->client_data;
    engine_->SendFuncWorkerMessage(op->client_id, &response);
    log_op_pool_.Return(op);
}

void SLogEngine::LogPersisted(std::unique_ptr<log::LogEntry> log_entry) {
    mu_.AssertHeld();
    if (log::LocalIdToNodeId(log_entry->localid) == engine_->node_id()) {
        LogOp* op = GrabAppendLogOp(log_entry->localid);
        if (op == nullptr) {
            return;
        }
        HVLOG(1) << fmt::format("Log (localid {}) replicated with seqnum {}",
                                log_entry->localid, log_entry->seqnum);
        Message response = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogOpType::APPEND_OK, op->client_data, log_entry->seqnum);
        engine_->SendFuncWorkerMessage(op->client_id, &response);
        log_op_pool_.Return(op);
    }
    storage_->Add(std::move(log_entry));
}

void SLogEngine::LogDiscarded(std::unique_ptr<log::LogEntry> log_entry) {
    mu_.AssertHeld();
    if (log::LocalIdToNodeId(log_entry->localid) == engine_->node_id()) {
        LogOp* op = GrabAppendLogOp(log_entry->localid);
        if (op == nullptr) {
            return;
        }
        HLOG(WARNING) << fmt::format("Log with localid {} discarded", log_entry->localid);
        Message response = MessageHelper::NewSharedLogOpFailed(
            SharedLogOpType::DISCARDED, op->client_data);
        engine_->SendFuncWorkerMessage(op->client_id, &response);
        log_op_pool_.Return(op);
    }
}

void SLogEngine::ReadLogFromStorage(uint64_t seqnum, uint64_t client_data,
                                    protocol::Message* message) {
    std::span<const char> data;
    if (storage_->Read(seqnum, &data)) {
        if (data.size() > MESSAGE_INLINE_DATA_SIZE) {
            HLOG(FATAL) << "Log data too long to fit into one message, "
                            "this should not happend given current implementation";
        }
        *message = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogOpType::READ_OK, client_data, seqnum);
        MessageHelper::SetInlineData(message, data);
    } else {
        *message = MessageHelper::NewSharedLogOpFailed(
            SharedLogOpType::DATA_LOST, client_data);
    }
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

void SLogEngine::SendMessageToEngine(uint16_t node_id, const protocol::Message& message) {
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    SLogMessageHub* hub = DCHECK_NOTNULL(
        io_worker->PickConnection(kSLogMessageHubTypeId))->as_ptr<SLogMessageHub>();
    hub->SendMessage(node_id, message);
}

void SLogEngine::ScheduleLocalCut(int duration_us) {
    mu_.AssertHeld();
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* timer = io_worker->PickConnection(kSLogEngineTimerTypeId);
    DCHECK(timer != nullptr);
    timer->as_ptr<Timer>()->TriggerIn(duration_us);
}

std::string_view SLogEngine::GetNodeAddr(uint16_t node_id) {
    absl::ReaderMutexLock lk(&mu_);
    return DCHECK_NOTNULL(core_.fsm()->current_view())->get_addr(node_id);
}

}  // namespace engine
}  // namespace faas
