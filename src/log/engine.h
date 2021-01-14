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

    absl::Mutex view_mu_;
    const View* current_view_        ABSL_GUARDED_BY(view_mu_);
    bool current_view_active_        ABSL_GUARDED_BY(view_mu_);
    std::vector<const View*> views_  ABSL_GUARDED_BY(view_mu_);
    LogSpaceCollection<LogProducer>
        producer_collection_         ABSL_GUARDED_BY(view_mu_);

    absl::Mutex future_request_mu_   ABSL_ACQUIRED_AFTER(view_mu_);
    log_utils::FutureRequests
        future_requests_             ABSL_GUARDED_BY(future_request_mu_);
    
    absl::Mutex pending_appends_mu_  ABSL_ACQUIRED_AFTER(view_mu_);
    std::map</* op_id */ uint64_t, LocalOp*>
        pending_appends_             ABSL_GUARDED_BY(pending_appends_mu_);

    void OnViewCreated(const View* view) override;
    void OnViewFrozen(const View* view) override;
    void OnViewFinalized(const FinalizedView* finalized_view) override;

    void HandleLocalAppend(LocalOp* op) override;
    void HandleLocalTrim(LocalOp* op) override;
    void HandleLocalRead(LocalOp* op) override;

    void HandleRemoteRead(const protocol::SharedLogMessage& request) override;
    void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                           std::span<const char> payload) override;
    void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                            std::span<const char> payload) override;
    void OnRecvResponse(const protocol::SharedLogMessage& message,
                        std::span<const char> payload) override;

    void ProcessAppendResults(const LogProducer::AppendResultVec& results);
    void ProcessRequests(const std::vector<SharedLogRequest>& requests);

    inline uint16_t GetLastViewId(LocalOp* op) {
        return bits::HighHalf32(bits::HighHalf64(op->metalog_progress));
    }

    inline LogMetaData MetaDataFromAppendOp(LocalOp* op) {
        DCHECK(op->type == protocol::SharedLogOpType::APPEND);
        return LogMetaData {
            .user_logspace = op->user_logspace,
            .user_tag = op->user_tag,
            .seqnum = 0,
            .localid = 0
        };
    }

    DISALLOW_COPY_AND_ASSIGN(Engine);
};

}  // namespace log
}  // namespace faas
