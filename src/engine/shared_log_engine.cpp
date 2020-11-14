#include "engine/shared_log_engine.h"

#include "engine/engine.h"

#define HLOG(l) LOG(l) << "SharedLogEngine: "
#define HVLOG(l) VLOG(l) << "SharedLogEngine: "

namespace faas {
namespace engine {

using protocol::Message;
using protocol::MessageHelper;
using protocol::GatewayMessage;
using protocol::GatewayMessageHelper;
using protocol::SharedLogOpType;

SharedLogEngine::SharedLogEngine(Engine* engine)
    : engine_(engine),
      core_(engine->node_id()),
      storage_(new log::InMemoryStorage()) {
    core_.SetLogPersistedCallback([this] (std::unique_ptr<log::LogEntry> log_entry) {
        storage_->Add(std::move(log_entry));
    });
    core_.SetLogDiscardedCallback(
        absl::bind_front(&SharedLogEngine::LogDiscarded, this));
    core_.SetAppendBackupLogCallback(
        absl::bind_front(&SharedLogEngine::AppendBackupLog, this));
    core_.SetSendSequencerMessageCallback(
        absl::bind_front(&SharedLogEngine::SendSequencerMessage, this));
}

SharedLogEngine::~SharedLogEngine() {}

void SharedLogEngine::OnSequencerMessage(std::span<const char> data) {
    log::SequencerMsgProto message_proto;
    if (!message_proto.ParseFromArray(data.data(), data.size())) {
        HLOG(ERROR) << "Failed to parse sequencer message!";
        return;
    }
    absl::MutexLock lk(&mu_);
    core_.NewSequencerMessage(message_proto);
}

void SharedLogEngine::OnMessageFromOtherEngine(const protocol::Message& message) {
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

void SharedLogEngine::OnMessageFromFuncWorker(const protocol::Message& message) {
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

void SharedLogEngine::LogDiscarded(std::unique_ptr<log::LogEntry> log_entry) {
    HLOG(INFO) << fmt::format("Log with localid {} discarded", log_entry->localid);
    // TODO
}

void SharedLogEngine::AppendBackupLog(uint16_t view_id, uint16_t backup_node_id,
                                      const log::LogEntry* log_entry) {
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* hub = io_worker->PickConnection(SharedLogMessageHub::kTypeId);
    DCHECK(hub != nullptr);
    Message message = MessageHelper::NewSharedLogAppend(log_entry->tag, log_entry->localid);
    MessageHelper::SetInlineData(&message, log_entry->data);
    hub->as_ptr<SharedLogMessageHub>()->SendMessage(view_id, backup_node_id, message);
}

void SharedLogEngine::SendSequencerMessage(std::span<const char> data) {
    // GatewayMessage message = GatewayMessageHelper::NewSharedLogOp();
    // message.payload_size = data.size();
    // engine_->SendGatewayMessage(message, data);
}

std::string_view SharedLogEngine::GetNodeAddr(uint16_t view_id, uint16_t node_id) {
    absl::ReaderMutexLock lk(&mu_);
    return DCHECK_NOTNULL(core_.fsm()->view_with_id(view_id))->get_addr(node_id);
}

}  // namespace engine
}  // namespace faas
