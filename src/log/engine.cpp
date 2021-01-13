#include "log/engine.h"

#include "engine/engine.h"
#include "utils/bits.h"

namespace faas {
namespace log {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

Engine::Engine(engine::Engine* engine)
    : EngineBase(engine),
      log_header_(fmt::format("LogEngine[{}-N]: ", my_node_id())),
      current_view_(nullptr) {}

Engine::~Engine() {}

void Engine::OnViewCreated(const View* view) {

}

void Engine::OnViewFinalized(const FinalizedView* finalized_view) {

}

void Engine::HandleLocalAppend(const Message& message) {

}

void Engine::HandleLocalTrim(const Message& message) {

}

void Engine::HandleLocalRead(const Message& message) {

}

void Engine::HandleRemoteRead(const SharedLogMessage& message) {

}

void Engine::OnRecvNewMetaLogs(const SharedLogMessage& message,
                               std::span<const char> payload) {

}

void Engine::OnRecvNewIndexData(const SharedLogMessage& message,
                                std::span<const char> payload) {

}

void Engine::OnRecvResponse(const SharedLogMessage& message,
                            std::span<const char> payload) {

}

void Engine::ProcessRequests(const std::vector<SharedLogRequest>& requests) {

}

}  // namespace log
}  // namespace faas
