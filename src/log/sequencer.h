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

    absl::Mutex core_mu_;
    const View* current_view_ ABSL_GUARDED_BY(core_mu_);

    LockablePtr<MetaLogPrimary>        current_primary_    ABSL_GUARDED_BY(core_mu_);
    LogSpaceCollection<MetaLogPrimary> primary_collection_ ABSL_GUARDED_BY(core_mu_);
    LogSpaceCollection<MetaLogBackup>  backup_collection_  ABSL_GUARDED_BY(core_mu_);

    absl::Mutex future_request_mu_ ABSL_ACQUIRED_AFTER(core_mu_);
    log_utils::FutureRequests future_requests_ ABSL_GUARDED_BY(future_request_mu_);

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

    void MarkNextCutIfDoable() override;

    DISALLOW_COPY_AND_ASSIGN(Sequencer);
};

}  // namespace log
}  // namespace faas
