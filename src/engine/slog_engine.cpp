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
using protocol::SequencerMessage;
using protocol::SequencerMessageHelper;

SLogEngine::SLogEngine(Engine* engine)
    : engine_(engine),
      sequencer_config_(&engine->sequencer_config_),
      core_(engine->node_id()) {
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
    } else if (op_type == SharedLogOpType::APPEND_OK || op_type == SharedLogOpType::DISCARDED) {
        RemoteAppendFinished(message);
    } else if (op_type == SharedLogOpType::READ_OK || op_type == SharedLogOpType::DATA_LOST) {
        RemoteReadAtFinished(message);
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
    } else {
        HLOG(FATAL) << "Unknown SharedLogOpType from local message: "
                    << static_cast<uint16_t>(op_type);
    }
}

void SLogEngine::HandleRemoteAppend(const protocol::Message& message) {
    DCHECK(message.src_node_id != my_node_id());
    std::span<const char> data = MessageHelper::GetInlineData(message);
    LogOp* op = AllocLogOp(LogOpType::kAppend, /* client_id= */ 0, message.log_client_data);
    op->log_tag = message.log_tag;
    op->src_node_id = message.src_node_id;
    NewAppendLogOp(op, data);
}

void SLogEngine::HandleRemoteReplicate(const Message& message) {
    absl::MutexLock lk(&mu_);
    core_.StoreLogAsBackupNode(message.log_tag, MessageHelper::GetInlineData(message),
                               message.log_localid);
}

void SLogEngine::HandleRemoteReadAt(const protocol::Message& message) {
    Message response;
    ReadLogFromStorage(message.log_seqnum, &response);
    response.log_client_data = message.log_client_data;
    SendMessageToEngine(message.src_node_id, &response);
}

void SLogEngine::HandleLocalAppend(const Message& message) {
    std::span<const char> data = MessageHelper::GetInlineData(message);
    if (data.empty()) {
        SendFailedResponse(message, SharedLogOpType::BAD_ARGS);
        return;
    }
    LogOp* op = AllocLogOp(LogOpType::kAppend, message.log_client_id, message.log_client_data);
    op->log_tag = message.log_tag;
    NewAppendLogOp(op, data);
}

void SLogEngine::HandleLocalReadNext(const protocol::Message& message) {
    if (message.log_tag != log::kDefaultLogTag) {
        HLOG(ERROR) << "Cannot handle non-zero log tag at the moment";
        SendFailedResponse(message, SharedLogOpType::BAD_ARGS);
        return;
    }

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
        SendFailedResponse(message, SharedLogOpType::EMPTY);
        return;
    }

    LogOp* op = AllocLogOp(LogOpType::kRead, message.log_client_id, message.log_client_data);
    op->log_tag = message.log_tag;
    op->log_seqnum = seqnum;
    NewReadLogOp(op, view, primary_node_id);
}

void SLogEngine::HandleLocalCheckTail(const protocol::Message& message) {
    if (message.log_tag != log::kDefaultLogTag) {
        HLOG(ERROR) << "Cannot handle non-zero log tag at the moment";
        SendFailedResponse(message, SharedLogOpType::BAD_ARGS);
        return;
    }

    const log::Fsm::View* view;
    uint64_t seqnum;
    uint16_t primary_node_id;
    bool success = false;
    {
        absl::ReaderMutexLock lk(&mu_);
        success = core_.fsm()->CheckTail(&seqnum, &view, &primary_node_id);
    }

    if (!success) {
        SendFailedResponse(message, SharedLogOpType::EMPTY);
        return;
    }

    LogOp* op = AllocLogOp(LogOpType::kRead, message.log_client_id, message.log_client_data);
    op->log_tag = message.log_tag;
    op->log_seqnum = seqnum;
    NewReadLogOp(op, view, primary_node_id);
}

void SLogEngine::RemoteAppendFinished(const protocol::Message& message) {
    LogOp* op = nullptr;
    {
        absl::MutexLock lk(&mu_);
        op = GrabLogOp(remote_append_ops_, message.log_client_data);
    }
    if (op == nullptr) {
        return;
    }
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::DISCARDED && --op->remaining_retries > 0) {
        std::span<const char> data(op->log_data.data(), op->log_data.size());
        NewAppendLogOp(op, data);
    } else {
        Message response = message;
        FinishLogOp(op, &response);
    }
}

void SLogEngine::RemoteReadAtFinished(const protocol::Message& message) {
    LogOp* op = nullptr;
    {
        absl::MutexLock lk(&mu_);
        op = GrabLogOp(remote_read_ops_, message.log_client_data);
    }
    if (op == nullptr) {
        return;
    }
    Message response = message;
    response.log_seqnum = op->log_seqnum;
    FinishLogOp(op, &response);
}

void SLogEngine::LogPersisted(std::unique_ptr<log::LogEntry> log_entry) {
    mu_.AssertHeld();
    if (log::LocalIdToNodeId(log_entry->localid) == my_node_id()) {
        LogOp* op = GrabLogOp(append_ops_, log_entry->localid);
        if (op == nullptr) {
            return;
        }
        HVLOG(1) << fmt::format("Log (localid {}) replicated with seqnum {}",
                                log_entry->localid, log_entry->seqnum);
        Message response = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogOpType::APPEND_OK, log_entry->seqnum);
        FinishLogOp(op, &response);
    }
    storage_->Add(std::move(log_entry));
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
            NewAppendLogOp(op, data);
        } else {
            Message response = MessageHelper::NewSharedLogOpFailed(SharedLogOpType::DISCARDED);
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

void SLogEngine::NewReadLogOp(LogOp* op, const log::Fsm::View* view, uint16_t primary_node_id) {
    if (view->IsStorageNodeOf(primary_node_id, my_node_id())) {
        HVLOG(1) << fmt::format("Find log (seqnum={}) locally", op->log_seqnum);
        Message response;
        ReadLogFromStorage(op->log_seqnum, &response);
        FinishLogOp(op, &response);
        return;
    }
    {
        absl::MutexLock lk(&mu_);
        remote_read_ops_[op->id] = op;
    }
    Message message = MessageHelper::NewSharedLogReadAt(op->log_seqnum, op->id);
    SendMessageToEngine(view->PickOneStorageNode(primary_node_id), &message);
}

void SLogEngine::NewAppendLogOp(LogOp* op, std::span<const char> data) {
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
                remote_append_ops_[op->id] = op;
            }
        }
    } while (0);
    if (!success) {
        Message message = MessageHelper::NewSharedLogOpFailed(SharedLogOpType::DISCARDED);
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
        if (data.size() > MESSAGE_INLINE_DATA_SIZE) {
            HLOG(FATAL) << "Log data too long to fit into one message, "
                            "this should not happend given current implementation";
        }
        *response = MessageHelper::NewSharedLogOpSucceeded(
            SharedLogOpType::READ_OK, seqnum);
        MessageHelper::SetInlineData(response, data);
    } else {
        *response = MessageHelper::NewSharedLogOpFailed(SharedLogOpType::DATA_LOST);
    }
}

void SLogEngine::SendFailedResponse(const protocol::Message& request,
                                    SharedLogOpType reason) {
    Message response = MessageHelper::NewSharedLogOpFailed(reason);
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
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    SLogMessageHub* hub = DCHECK_NOTNULL(
        io_worker->PickConnection(kSLogMessageHubTypeId))->as_ptr<SLogMessageHub>();
    message->src_node_id = my_node_id();
    hub->SendMessage(node_id, *message);
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
