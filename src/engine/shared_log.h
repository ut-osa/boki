#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "engine/io_worker.h"

namespace faas {
namespace engine {

class Engine;

class IncomingSharedLogConnection final : public ConnectionBase {
public:
    static constexpr int kTypeId = 2;
    static constexpr uint64_t kBufGroup = 3;
    static constexpr size_t kBufSize = __FAAS_MESSAGE_SIZE * 4;

    IncomingSharedLogConnection(Engine* engine, int sockfd);
    ~IncomingSharedLogConnection();

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    Engine* engine_;
    IOWorker* io_worker_;
    State state_;
    int sockfd_;

    std::string log_header_;

    utils::AppendableBuffer message_buffer_;

    bool OnRecvData(int status, std::span<const char> data);

    DISALLOW_COPY_AND_ASSIGN(IncomingSharedLogConnection);
};

class SharedLogMessageHub final : public ConnectionBase {
public:
    static constexpr int kTypeId = 3;

    explicit SharedLogMessageHub(Engine* engine);
    ~SharedLogMessageHub();

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    void SendMessage(uint16_t view_id, uint16_t node_id,
                     const protocol::Message& message);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    Engine* engine_;
    IOWorker* io_worker_;
    State state_;

    std::string log_header_;

    class Connection;
    struct NodeContext;
    absl::flat_hash_map</* node_id */ uint16_t,
                        std::unique_ptr<NodeContext>> node_ctxes_;
    absl::flat_hash_set<std::unique_ptr<Connection>> connections_;

    void SetupConnections(uint16_t view_id, uint16_t node_id);
    void OnConnectionConnected(Connection* connection);
    void OnConnectionClosing(Connection* connection);
    void OnConnectionClosed(Connection* connection);

    DISALLOW_COPY_AND_ASSIGN(SharedLogMessageHub);
};

}  // namespace engine
}  // namespace faas
