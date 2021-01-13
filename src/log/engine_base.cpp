#include "log/engine_base.h"

#include "log/flags.h"
#include "server/constants.h"
#include "engine/engine.h"
#include "utils/bits.h"

#define log_header_ "LogEngineBase: "

namespace faas {
namespace log {

using protocol::FuncCall;
using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

EngineBase::EngineBase(engine::Engine* engine)
    : node_id_(engine->node_id_),
      engine_(engine) {}

EngineBase::~EngineBase() {}

zk::ZKSession* EngineBase::zk_session() {
    return engine_->zk_session();
}

void EngineBase::Start() {
    SetupZKWatchers();
    SetupTimers();
}

void EngineBase::Stop() {}

void EngineBase::SetupZKWatchers() {
    view_watcher_.SetViewCreatedCallback(
        [this] (const View* view) {
            this->OnViewCreated(view);
        }
    );
    view_watcher_.SetViewFinalizedCallback(
        [this] (const FinalizedView* finalized_view) {
            this->OnViewFinalized(finalized_view);
        }
    );
    view_watcher_.StartWatching(zk_session());
}

void EngineBase::SetupTimers() {
}

void EngineBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                        const SharedLogMessage& message,
                                        std::span<const char> payload) {

}

void EngineBase::OnNewExternalFuncCall(const FuncCall& func_call, uint32_t log_space) {

}

void EngineBase::OnNewInternalFuncCall(const FuncCall& func_call,
                                       const FuncCall& parent_func_call) {

}

void EngineBase::OnFuncCallCompleted(const FuncCall& func_call) {

}

void EngineBase::OnMessageFromFuncWorker(const Message& message) {

}

void EngineBase::SharedLogMessageHandler(const SharedLogMessage& message,
                                         std::span<const char> payload) {

}

void EngineBase::LocalMessageHandler(const Message& message) {

}

void EngineBase::ReplicateLogEntry(const View* view, const LogMetaData& log_metadata,
                                   std::span<const char> log_data) {

}

bool EngineBase::SendReadRequest(uint16_t engine_id, SharedLogMessage* message) {
    NOT_IMPLEMENTED();
}

bool EngineBase::SendSequencerMessage(uint16_t sequencer_id,
                                      SharedLogMessage* message,
                                      std::span<const char> payload) {
    NOT_IMPLEMENTED();  
}

bool EngineBase::SendEngineResponse(const protocol::SharedLogMessage& request,
                                   SharedLogMessage* response,
                                   std::span<const char> payload) {
    NOT_IMPLEMENTED();
}

}  // namespace log
}  // namespace faas
