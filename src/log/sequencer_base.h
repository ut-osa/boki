#pragma once

#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"
#include "server/server_base.h"
#include "server/ingress_connection.h"
#include "server/egress_hub.h"

namespace faas {
namespace log {

class SequencerBase : public server::ServerBase {
public:
    explicit SequencerBase(uint16_t node_id);
    virtual ~SequencerBase();

protected:
    uint16_t my_node_id() const { return node_id_; }

    virtual void OnViewCreated(const View* view) = 0;
    virtual void OnViewFrozen(const View* view) = 0;
    virtual void OnViewFinalized(const FinalizedView* finalized_view) = 0;

    virtual void HandleTrimRequest(const protocol::SharedLogMessage& message) = 0;
    virtual void OnRecvMetaLogProgress(const protocol::SharedLogMessage& message) = 0;
    virtual void OnRecvShardProgress(const protocol::SharedLogMessage& message,
                                     std::span<const char> payload) = 0;
    virtual void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                                   std::span<const char> payload) = 0;

    virtual void MarkNextCutIfDoable() = 0;

    void MessageHandler(const protocol::SharedLogMessage& message,
                        std::span<const char> payload);

    void ReplicateMetaLog(const View* view, const MetaLogProto& metalog);
    void PropagateMetaLog(const View* view, const MetaLogProto& metalog);

    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN);
    bool SendEngineResponse(const protocol::SharedLogMessage& request,
                            protocol::SharedLogMessage* response,
                            std::span<const char> payload = EMPTY_CHAR_SPAN);

private:
    const uint16_t node_id_;

    ViewWatcher view_watcher_;

    absl::flat_hash_map</* id */ int, std::unique_ptr<server::IngressConnection>>
        ingress_conns_;

    absl::Mutex conn_mu_;
    absl::flat_hash_map</* id */ int, std::unique_ptr<server::EgressHub>>
        egress_hubs_ ABSL_GUARDED_BY(conn_mu_);

    void SetupZKWatchers();
    void SetupTimers();

    void StartInternal() override;
    void StopInternal() override;
    void OnConnectionClose(server::ConnectionBase* connection) override;
    void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                             int sockfd) override;

    void OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);
    bool SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                              const protocol::SharedLogMessage& message,
                              std::span<const char> payload);

    server::EgressHub* CreateEgressHub(protocol::ConnType conn_type,
                                       uint16_t dst_node_id,
                                       server::IOWorker* io_worker);

    DISALLOW_COPY_AND_ASSIGN(SequencerBase);
};

}  // namespace log
}  // namespace faas
