#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "engine/io_worker.h"

namespace faas {
namespace engine {

class Engine;

class GatewayConnection final : public ConnectionBase {
public:
    static constexpr size_t kBufSize = 65536;

    GatewayConnection(Engine* engine, uint16_t conn_id, int sockfd);
    ~GatewayConnection();

    uint16_t conn_id() const { return conn_id_; }

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    void SendMessage(const protocol::GatewayMessage& message,
                     std::span<const char> payload);

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    Engine* engine_;
    uint16_t conn_id_;
    IOWorker* io_worker_;
    State state_;
    int sockfd_;

    std::string log_header_;

    protocol::GatewayMessage handshake_message_;
    utils::AppendableBuffer read_buffer_;

    void ProcessGatewayMessages();
    bool OnRecvData(int status, std::span<const char> data);

    DISALLOW_COPY_AND_ASSIGN(GatewayConnection);
};

}  // namespace engine
}  // namespace faas
