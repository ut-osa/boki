#pragma once

#include "common/zk.h"
#include "log/common.h"
#include "log/view.h"
#include "log/view_watcher.h"

namespace faas {

// Forward declarations
namespace engine {
class Engine;
}  // namespace engine

namespace log {

class EngineBase {
public:
    explicit EngineBase(engine::Engine* engine);
    virtual ~EngineBase();

    void Start();
    void Stop();

    void OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                const protocol::SharedLogMessage& message,
                                std::span<const char> payload);

    void OnNewExternalFuncCall(const protocol::FuncCall& func_call, uint32_t log_space);
    void OnNewInternalFuncCall(const protocol::FuncCall& func_call,
                               const protocol::FuncCall& parent_func_call);
    void OnFuncCallCompleted(const protocol::FuncCall& func_call);
    void OnMessageFromFuncWorker(const protocol::Message& message);

protected:
    uint16_t my_node_id() const { return node_id_; }
    zk::ZKSession* zk_session();

    virtual void OnViewCreated(const View* view) = 0;
    virtual void OnViewFinalized(const FinalizedView* finalized_view) = 0;

    virtual void HandleLocalAppend(const protocol::Message& message) = 0;
    virtual void HandleLocalTrim(const protocol::Message& message) = 0;
    virtual void HandleLocalRead(const protocol::Message& message) = 0;

    virtual void HandleRemoteRead(const protocol::SharedLogMessage& message) = 0;
    virtual void OnRecvNewMetaLogs(const protocol::SharedLogMessage& message,
                                   std::span<const char> payload) = 0;
    virtual void OnRecvNewIndexData(const protocol::SharedLogMessage& message,
                                    std::span<const char> payload) = 0;
    virtual void OnRecvResponse(const protocol::SharedLogMessage& message,
                                std::span<const char> payload) = 0;

    void SharedLogMessageHandler(const protocol::SharedLogMessage& message,
                                 std::span<const char> payload);
    void LocalMessageHandler(const protocol::Message& message);

    void ReplicateLogEntry(const View* view, const LogMetaData& log_metadata,
                           std::span<const char> log_data);

    bool SendReadRequest(uint16_t engine_id, protocol::SharedLogMessage* message);
    bool SendSequencerMessage(uint16_t sequencer_id,
                              protocol::SharedLogMessage* message,
                              std::span<const char> payload = EMPTY_CHAR_SPAN);
    bool SendEngineResponse(const protocol::SharedLogMessage& request,
                            protocol::SharedLogMessage* response,
                            std::span<const char> payload = EMPTY_CHAR_SPAN);

private:
    const uint16_t node_id_;
    engine::Engine* engine_;

    ViewWatcher view_watcher_;

    void SetupZKWatchers();
    void SetupTimers();

    DISALLOW_COPY_AND_ASSIGN(EngineBase);
};

}  // namespace log
}  // namespace faas
