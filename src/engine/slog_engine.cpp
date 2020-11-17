#include "engine/slog_engine.h"

#include "common/time.h"
#include "engine/constants.h"
#include "engine/sequencer_connection.h"
#include "engine/engine.h"

#define HLOG(l) LOG(l) << "SLogEngine: "
#define HVLOG(l) VLOG(l) << "SLogEngine: "

namespace faas {
namespace engine {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogOpType;
using protocol::SequencerMessage;
using protocol::SequencerMessageHelper;

SLogEngine::SLogEngine(Engine* engine)
    : engine_(engine),
      core_(engine->node_id()),
      storage_(new log::InMemoryStorage()) {
    core_.SetLogPersistedCallback([this] (std::unique_ptr<log::LogEntry> log_entry) {
        storage_->Add(std::move(log_entry));
    });
    core_.SetLogDiscardedCallback(
        absl::bind_front(&SLogEngine::LogDiscarded, this));
    core_.SetAppendBackupLogCallback(
        absl::bind_front(&SLogEngine::AppendBackupLog, this));
    core_.SetSendLocalCutMessageCallback(
        absl::bind_front(&SLogEngine::SendLocalCutMessage, this));
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
    absl::MutexLock lk(&mu_);
    core_.MarkAndSendLocalCut();
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

void SLogEngine::OnMessageFromOtherEngine(const protocol::Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        absl::MutexLock lk(&mu_);
        core_.NewRemoteLog(message.log_localid, message.log_tag,
                           MessageHelper::GetInlineData(message));
    } else if (op_type == SharedLogOpType::READ_AT) {
        // TODO
    } else if (op_type == SharedLogOpType::READ_NEXT) {
        // TODO
    } else if (op_type == SharedLogOpType::TRIM) {
        // TODO
    } else {
        HLOG(ERROR) << "Unknown SharedLogOpType";
    }
}

void SLogEngine::OnMessageFromFuncWorker(const protocol::Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        std::span<const char> data = MessageHelper::GetInlineData(message);
        uint64_t localid;
        absl::MutexLock lk(&mu_);
        if (!core_.NewLocalLog(message.log_tag, data, &localid)) {
            LOG(ERROR) << "NewLocalLog failed";
        }
    } else if (op_type == SharedLogOpType::READ_AT) {
        // TODO
    } else if (op_type == SharedLogOpType::READ_NEXT) {
        // TODO
    } else if (op_type == SharedLogOpType::TRIM) {
        // TODO
    } else {
        HLOG(ERROR) << "Unknown SharedLogOpType";
    }
}

void SLogEngine::LogDiscarded(std::unique_ptr<log::LogEntry> log_entry) {
    HLOG(INFO) << fmt::format("Log with localid {} discarded", log_entry->localid);
    // TODO
}

void SLogEngine::AppendBackupLog(uint16_t view_id, uint16_t backup_node_id,
                                 const log::LogEntry* log_entry) {
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* hub = io_worker->PickConnection(kSLogMessageHubTypeId);
    DCHECK(hub != nullptr);
    Message message = MessageHelper::NewSharedLogAppend(log_entry->tag, log_entry->localid);
    MessageHelper::SetInlineData(&message, log_entry->data);
    hub->as_ptr<SLogMessageHub>()->SendMessage(view_id, backup_node_id, message);
}

void SLogEngine::SendSequencerMessage(const protocol::SequencerMessage& message,
                                      std::span<const char> payload) {
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* conn = io_worker->PickConnection(kSequencerConnectionTypeId);
    if (conn == nullptr) {
        HLOG(ERROR) << "There is not SequencerConnection associated with current IOWorker";
        return;
    }
    conn->as_ptr<SequencerConnection>()->SendMessage(message, payload);
}

void SLogEngine::SendLocalCutMessage(std::span<const char> data) {
    SendSequencerMessage(SequencerMessageHelper::NewLocalCut(data), data);
}

std::string_view SLogEngine::GetNodeAddr(uint16_t view_id, uint16_t node_id) {
    absl::ReaderMutexLock lk(&mu_);
    return DCHECK_NOTNULL(core_.fsm()->view_with_id(view_id))->get_addr(node_id);
}

}  // namespace engine
}  // namespace faas
