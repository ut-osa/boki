#include "engine/slog_engine.h"

#include "common/time.h"
#include "log/common.h"
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
    core_.SetLogPersistedCallback(
        absl::bind_front(&SLogEngine::LogPersisted, this));
    core_.SetLogDiscardedCallback(
        absl::bind_front(&SLogEngine::LogDiscarded, this));
    core_.SetScheduleLocalCutCallback(
        absl::bind_front(&SLogEngine::ScheduleLocalCut, this));
    SetupTimers();
}

SLogEngine::~SLogEngine() {}

std::unique_ptr<SLogEngine::OngoingLogContext> SLogEngine::GrabOngoingLogContext(uint64_t localid) {
    if (!ongoing_logs_.contains(localid)) {
        HLOG(WARNING) << fmt::format("Cannot find ongoing log (localid {})", localid);
        return nullptr;
    }
    std::unique_ptr<OngoingLogContext> ctx = std::move(ongoing_logs_[localid]);
    ongoing_logs_.erase(localid);
    return ctx;
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
        core_.NewFsmRecordsMessage(message_proto);
    } else {
        HLOG(ERROR) << fmt::format("Unknown message type: {}!", message.message_type);
    }
}

void SLogEngine::OnMessageFromOtherEngine(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        HandleRemoteAppend(message);
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

void SLogEngine::OnMessageFromFuncWorker(const Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        HandleLocalAppend(message);
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

void SLogEngine::HandleRemoteAppend(const Message& message) {
    absl::MutexLock lk(&mu_);
    core_.NewRemoteLog(message.log_localid, message.log_tag,
                       MessageHelper::GetInlineData(message));
}

void SLogEngine::HandleLocalAppend(const Message& message) {
    std::unique_ptr<OngoingLogContext> ctx(new OngoingLogContext);
    ctx->client_id = message.log_client_id;
    ctx->client_data = message.log_client_data;
    ctx->log_tag = message.log_tag;
    std::span<const char> data = MessageHelper::GetInlineData(message);
    const log::Fsm::View* view;
    uint64_t localid;
    bool success = false;
    {
        absl::MutexLock lk(&mu_);
        success = core_.NewLocalLog(message.log_tag, data, &view, &localid);
        if (success) {
            ctx->log_localid = localid;
            ongoing_logs_[localid] = std::move(ctx);
        }
    }
    if (!success) {
        HLOG(ERROR) << "NewLocalLog failed";
        Message response = MessageHelper::NewSharedLogDiscarded(message.log_client_data);
        engine_->SendFuncWorkerMessage(message.log_client_id, &response);
        return;
    }

    // Replicate new log to backup nodes
    Message message_copy = message;
    message_copy.log_localid = localid;
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    SLogMessageHub* hub = DCHECK_NOTNULL(
        io_worker->PickConnection(kSLogMessageHubTypeId))->as_ptr<SLogMessageHub>();
    view->ForEachBackupNode(engine_->node_id(), [view, hub, &message_copy] (uint16_t node_id) {
        hub->SendMessage(view->id(), node_id, message_copy);
    });
}

void SLogEngine::LogPersisted(std::unique_ptr<log::LogEntry> log_entry) {
    mu_.AssertHeld();
    if (log::LocalIdToNodeId(log_entry->localid) == engine_->node_id()) {
        std::unique_ptr<OngoingLogContext> ctx = GrabOngoingLogContext(log_entry->localid);
        if (ctx == nullptr) {
            return;
        }
        HVLOG(1) << fmt::format("Log (localid {}) replicated with seqnum {}",
                                log_entry->localid, log_entry->seqnum);
        Message response = MessageHelper::NewSharedLogPersisted(ctx->client_data,
                                                                log_entry->seqnum);
        engine_->SendFuncWorkerMessage(ctx->client_id, &response);
    }
    storage_->Add(std::move(log_entry));
}

void SLogEngine::LogDiscarded(std::unique_ptr<log::LogEntry> log_entry) {
    mu_.AssertHeld();
    std::unique_ptr<OngoingLogContext> ctx = GrabOngoingLogContext(log_entry->localid);
    if (ctx == nullptr) {
        return;
    }
    HLOG(WARNING) << fmt::format("Log with localid {} discarded", log_entry->localid);
    Message response = MessageHelper::NewSharedLogDiscarded(ctx->client_data);
    engine_->SendFuncWorkerMessage(ctx->client_id, &response);
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

void SLogEngine::ScheduleLocalCut(int duration_us) {
    mu_.AssertHeld();
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* timer = io_worker->PickConnection(kSLogEngineTimerTypeId);
    DCHECK(timer != nullptr);
    timer->as_ptr<Timer>()->TriggerIn(duration_us);
}

std::string_view SLogEngine::GetNodeAddr(uint16_t view_id, uint16_t node_id) {
    absl::ReaderMutexLock lk(&mu_);
    return DCHECK_NOTNULL(core_.fsm()->view_with_id(view_id))->get_addr(node_id);
}

}  // namespace engine
}  // namespace faas
