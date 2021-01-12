#include "log/sequencer.h"

#include "utils/bits.h"

#define log_header_ "Storage: "

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

Sequencer::Sequencer(uint16_t node_id)
    : SequencerBase(node_id),
      current_view_(nullptr) {}

Sequencer::~Sequencer() {}

void Sequencer::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
}

void Sequencer::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
}

void Sequencer::HandleTrimRequest(const SharedLogMessage& request) {
    DCHECK(SharedLogMessageHelper::GetOpType(request) == SharedLogOpType::TRIM);
}

void Sequencer::OnRecvMetaLogProgress(const SharedLogMessage& message) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::META_PROG);
}

void Sequencer::OnRecvShardProgress(const SharedLogMessage& message,
                                    std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::SHARD_PROG);
}

void Sequencer::OnRecvNewMetaLogs(const SharedLogMessage& message,
                                  std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
}

void Sequencer::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_TO_SPAN(request.payload));
    }
}

void Sequencer::MarkNextCutIfDoable() {

}

}  // namespace log
}  // namespace faas
