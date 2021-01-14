#pragma once

#include "log/engine_base.h"
#include "log/log_space.h"
#include "log/utils.h"

namespace faas {

// Forward declarations
namespace engine {
class Engine;
}  // namespace engine

namespace log {

class Engine final : public EngineBase {
public:
    explicit Engine(engine::Engine* engine);
    ~Engine();

private:
    std::string log_header_;

    absl::Mutex core_mu_;
    const View* current_view_ ABSL_GUARDED_BY(core_mu_);

    absl::Mutex future_request_mu_ ABSL_ACQUIRED_AFTER(core_mu_);
    log_utils::FutureRequests future_requests_ ABSL_GUARDED_BY(future_request_mu_);

    void OnViewCreated(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleLocalAppend(LocalOp* op) override;
    void HandleLocalTrim(LocalOp* op) override;
    void HandleLocalRead(LocalOp* op) override;

    void HandleRemoteRead(const protocol::SharedLogMessage& message) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;
    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;
    void OnRecvResponse(const protocol::SharedLogMessage& message,
                        std::span<const char> payload) override;

    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

}  // namespace log
}  // namespace faas
