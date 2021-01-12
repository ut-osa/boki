#include "log/sequencer.h"

#include "utils/bits.h"

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

Sequencer::Sequencer(uint16_t node_id)
    : SequencerBase(node_id),
      log_header_(fmt::format("Sequencer[{}]: ", node_id)),
      current_view_(nullptr) {}

Sequencer::~Sequencer() {}

void Sequencer::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    bool contains_myself = view->contains_sequencer_node(my_node_id());
    std::vector<std::unique_ptr<MetaLogBackup>> new_backups;
    if (contains_myself) {
        for (uint16_t id : view->GetSequencerNodes()) {
            if (view->GetSequencerNode(id)->IsReplicaSequencerNode(my_node_id())) {
                new_backups.push_back(std::make_unique<MetaLogBackup>(view, id));
            }
        }
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock core_lk(&core_mu_);
        std::vector<std::unique_ptr<MetaLogPrimary>> new_primary;
        if (contains_myself) {
            new_primary.push_back(std::make_unique<MetaLogPrimary>(view, my_node_id()));
        }
        primary_collection_.OnNewView(view, std::move(new_primary));
        current_primary_ = primary_collection_.GetLogSpace(
            bits::JoinTwo16(view->id(), my_node_id()));
        DCHECK(!contains_myself || current_primary_ != nullptr);
        backup_collection_.OnNewView(view, std::move(new_backups));
        {
            absl::MutexLock future_request_lk(&future_request_mu_);
            future_requests_.OnNewView(view, &ready_requests);
        }
        current_view_ = view;
    }
    if (!ready_requests.empty()) {
        if (!contains_myself) {
            HLOG(FATAL) << fmt::format("Have requests for view {} not including myself",
                                       view->id());
        }
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] {
                ProcessRequests(requests);
            }
        );
    }
}

void Sequencer::OnViewFrozen(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
}

void Sequencer::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
}

void Sequencer::HandleTrimRequest(const SharedLogMessage& request) {
    DCHECK(SharedLogMessageHelper::GetOpType(request) == SharedLogOpType::TRIM);
    NOT_IMPLEMENT():
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
