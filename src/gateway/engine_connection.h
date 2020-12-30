#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"
#include "gateway/constants.h"

namespace faas {
namespace gateway {

class Server;

class EngineConnection final : public server::ConnectionBase {
public:
    static constexpr size_t kBufSize = 65536;

    static int type_id(uint16_t node_id) { return kEngineConnectionTypeId + node_id; }

    EngineConnection(Server* server, uint16_t node_id, uint16_t conn_id, int sockfd);
    ~EngineConnection();

    uint16_t node_id() const { return node_id_; }
    uint16_t conn_id() const { return conn_id_; }

    void Start(server::IOWorker* io_worker) override;
    void ScheduleClose() override;

    server::IOWorker* io_worker() { return io_worker_; }
    void SendMessage(const protocol::GatewayMessage& message, std::span<const char> payload);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    Server* server_;
    uint16_t node_id_;
    uint16_t conn_id_;
    server::IOWorker* io_worker_;
    State state_;
    int sockfd_;

    std::string log_header_;

    utils::AppendableBuffer read_buffer_;

    void ProcessGatewayMessages();
    bool OnRecvData(int status, std::span<const char> data);

    DISALLOW_COPY_AND_ASSIGN(EngineConnection);
};

}  // namespace gateway
}  // namespace faas
