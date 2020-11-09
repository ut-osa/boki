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
      log_core_(engine->node_id()) {
    log_core_.SetLogReplicatedCallback(
        absl::bind_front(&SharedLogEngine::OnLogReplicated, this));
    log_core_.SetLogDiscardedCallback(
        absl::bind_front(&SharedLogEngine::OnLogDiscarded, this));
    log_core_.SetAppendBackupLogCallback(
        absl::bind_front(&SharedLogEngine::AppendBackupLog, this));
    log_core_.SetSendSequencerMessageCallback(
        absl::bind_front(&SharedLogEngine::SendSequencerMessage, this));
}

SharedLogEngine::~SharedLogEngine() {
}

void SharedLogEngine::OnSequencerMessage(int seqnum, std::span<const char> data) {
    log_core_.OnSequencerMessage(seqnum, data);
}

void SharedLogEngine::OnMessageFromOtherEngine(const protocol::Message& message) {
    DCHECK(MessageHelper::IsSharedLogOp(message));
    SharedLogOpType op_type = MessageHelper::GetSharedLogOpType(message);
    if (op_type == SharedLogOpType::APPEND) {
        log_core_.NewRemoteLog(message.log_localid, message.log_tag,
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
        if (!log_core_.NewLocalLog(message.log_tag, data, &localid)) {
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

void SharedLogEngine::OnLogReplicated(std::pair<uint64_t, uint64_t> localid_range,
                                      std::pair<uint64_t, uint64_t> seqnum_range) {
    DCHECK_EQ(localid_range.second - localid_range.first,
              seqnum_range.second - seqnum_range.first);
    // TODO
}

void SharedLogEngine::OnLogDiscarded(uint64_t localid) {
    // TODO
}

void SharedLogEngine::AppendBackupLog(uint16_t view_id, uint16_t backup_node_id,
                                      uint64_t log_localid, uint32_t log_tag,
                                      std::span<const char> data) {
    IOWorker* io_worker = IOWorker::current();
    DCHECK(io_worker != nullptr);
    ConnectionBase* hub = io_worker->PickConnection(SharedLogMessageHub::kTypeId);
    DCHECK(hub != nullptr);
    Message message = MessageHelper::NewSharedLogAppend(log_tag, log_localid);
    MessageHelper::SetInlineData(&message, data);
    hub->as_ptr<SharedLogMessageHub>()->SendMessage(view_id, backup_node_id, message);
}

void SharedLogEngine::SendSequencerMessage(std::span<const char> data) {
    engine_->SendGatewayMessage(GatewayMessageHelper::NewSharedLogOp(), data);
}

}  // namespace engine
}  // namespace faas
