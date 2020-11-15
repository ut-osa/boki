#include "sequencer/server.h"

#define HLOG(l) LOG(l) << "Server: "
#define HVLOG(l) VLOG(l) << "Server: "

namespace faas {
namespace sequencer {

using protocol::SequencerMessage;
using protocol::SequencerMessageHelper;

Server::Server()
    : state_(kCreated),
      event_loop_thread_("Server/EL", absl::bind_front(&Server::EventLoopThreadMain, this)),
      node_manager_(this) {
    UV_DCHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    UV_DCHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &Server::StopCallback));
    stop_event_.data = this;
}

Server::~Server() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    UV_DCHECK_OK(uv_loop_close(&uv_loop_));
}

void Server::Start() {

}

void Server::ScheduleStop() {

}

void Server::WaitForFinish() {

}

void Server::EventLoopThreadMain() {

}

void Server::OnNewNodeConnected(uint16_t node_id, std::string_view shared_log_addr) {

}

void Server::OnNodeDisconnected(uint16_t node_id) {

}

void Server::OnRecvNodeMessage(uint16_t node_id, const SequencerMessage& message,
                               std::span<const char> payload) {
        
}

UV_ASYNC_CB_FOR_CLASS(Server, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    HLOG(INFO) << "Start stopping process";
    node_manager_.ScheduleStop();
    uv_close(UV_AS_HANDLE(&stop_event_), nullptr);
    state_.store(kStopping);
}

}  // namespace sequencer
}  // namespace faas
