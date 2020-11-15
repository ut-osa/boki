#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "common/uv.h"
#include "log/sequencer_core.h"
#include "sequencer/node_manager.h"

namespace faas {
namespace sequencer {

class Server final : public uv::Base {
public:
    Server();
    ~Server();

    void set_address(std::string_view address) { address_ = std::string(address); }
    void set_engine_conn_port(int port) { engine_conn_port_ = port; }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnNewNodeConnected(uint16_t node_id, std::string_view shared_log_addr);
    void OnNodeDisconnected(uint16_t node_id);
    void OnRecvNodeMessage(uint16_t node_id, const protocol::SequencerMessage& message,
                           std::span<const char> payload);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    std::string address_;
    int engine_conn_port_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    NodeManager node_manager_;
    log::SequencerCore core_;

    void EventLoopThreadMain();

    void SendFsmRecordsMessage(uint16_t node_id, std::span<const char> data);

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);

    DISALLOW_COPY_AND_ASSIGN(Server);
};

}  // namespace sequencer
}  // namespace faas
