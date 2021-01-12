#pragma once

#include "base/thread.h"
#include "log/storage_base.h"
#include "log/log_space.h"
#include "log/utils.h"

namespace faas {
namespace log {

class Storage final : public StorageBase {
public:
    explicit Storage(uint16_t node_id);
    ~Storage();

private:
    absl::Mutex core_mu_;
    const View* current_view_ ABSL_GUARDED_BY(core_mu_);
    LogSpaceCollection<LogStorage> storage_collection_ ABSL_GUARDED_BY(core_mu_);

    absl::Mutex future_request_mu_ ABSL_ACQUIRED_AFTER(core_mu_);
    FutureRequests future_requests_ ABSL_GUARDED_BY(future_request_mu_);

    void OnViewCreated(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleReadAtRequest(const protocol::SharedLogMessage& message) override;
    void HandleReplicateRequest(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;

    void ProcessReadResults(const LogStorage::ReadResultVec& results);
    void ProcessReadFromDB(const protocol::SharedLogMessage& request);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    void BackgroundThreadMain() override;
    void SendShardProgressIfNeeded() override;

    DISALLOW_COPY_AND_ASSIGN(Storage);
};

}  // namespace log
}  // namespace faas
