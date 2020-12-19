#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "engine/io_worker.h"

namespace faas {
namespace engine {

class SLogEngine;

class IncomingSLogConnection final : public ConnectionBase {
public:
    static constexpr size_t kBufSize = __FAAS_MESSAGE_SIZE * 4;

    IncomingSLogConnection(SLogEngine* slog_engine, int sockfd);
    ~IncomingSLogConnection();

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    SLogEngine* slog_engine_;
    IOWorker* io_worker_;
    State state_;
    int sockfd_;

    std::string log_header_;

    utils::AppendableBuffer message_buffer_;
    protocol::Message received_message_;

    bool OnRecvData(int status, std::span<const char> data);

    DISALLOW_COPY_AND_ASSIGN(IncomingSLogConnection);
};

class SLogMessageHub final : public ConnectionBase {
public:
    explicit SLogMessageHub(SLogEngine* slog_engine);
    ~SLogMessageHub();

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    void SendMessage(uint16_t node_id, const protocol::Message& message);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    SLogEngine* slog_engine_;
    IOWorker* io_worker_;
    State state_;

    std::string log_header_;

    class Connection;
    struct NodeContext;
    absl::flat_hash_map</* node_id */ uint16_t,
                        std::unique_ptr<NodeContext>> node_ctxes_;
    absl::flat_hash_map<int, std::unique_ptr<Connection>> connections_;
    int next_connection_id_;

    void SetupConnections(uint16_t node_id);
    void OnConnectionConnected(Connection* connection);
    void OnConnectionClosing(Connection* connection);
    void OnConnectionClosed(Connection* connection);

    DISALLOW_COPY_AND_ASSIGN(SLogMessageHub);
};

}  // namespace engine
}  // namespace faas
