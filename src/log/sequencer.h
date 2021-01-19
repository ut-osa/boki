#pragma once

#include "log/sequencer_base.h"
#include "log/log_space.h"
#include "log/utils.h"

namespace faas {
namespace log {

class Sequencer final : public SequencerBase {
public:
    explicit Sequencer(uint16_t node_id);
    ~Sequencer();

private:
    std::string log_header_;

    absl::Mutex view_mu_;
    const View* current_view_          ABSL_GUARDED_BY(view_mu_);
    LockablePtr<MetaLogPrimary>
        current_primary_               ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<MetaLogPrimary>
        primary_collection_            ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<MetaLogBackup>
        backup_collection_             ABSL_GUARDED_BY(view_mu_);

    log_utils::FutureRequests future_requests_;

    void OnViewCreated(const View* view) override;
    void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleTrimRequest(const protocol::SharedLogMessage& request) override;
    void OnRecvMetaLogProgress(const protocol::SharedLogMessage& message) override;
    void OnRecvShardProgress(const protocol::SharedLogMessage& message,
                             std::span<const char> payload) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;

    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    template<class T>
    void FreezeLogSpace(LockablePtr<T> logspace_ptr);
    template<class T>
    void FinalizedLogSpace(LockablePtr<T> logspace_ptr,
                           const FinalizedView* finalized_view);

    void MarkNextCutIfDoable() override;

    DISALLOW_COPY_AND_ASSIGN(Sequencer);
};

template<class T>
void Sequencer::FreezeLogSpace(LockablePtr<T> logspace_ptr) {
    auto locked_logspace = logspace_ptr.Lock();
    locked_logspace->Freeze();
}

template<class T>
void Sequencer::FinalizedLogSpace(LockablePtr<T> logspace_ptr,
                                  const FinalizedView* finalized_view) {
    auto locked_logspace = logspace_ptr.Lock();
    uint16_t logspace_id = locked_logspace->identifier();
    bool success = locked_logspace->Finalize(
        finalized_view->final_metalog_position(logspace_id),
        finalized_view->tail_metalogs(logspace_id));
    if (!success) {
        HLOG(FATAL) << fmt::format("Failed to finalize log space {}",
                                    bits::HexStr0x(logspace_id));
    }
}

}  // namespace log
}  // namespace faas
