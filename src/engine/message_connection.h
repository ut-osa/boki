#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/stat.h"
#include "utils/appendable_buffer.h"
#include "utils/object_pool.h"
#include "engine/io_worker.h"

namespace faas {
namespace engine {

class Engine;

class MessageConnection final : public ConnectionBase {
public:
    static constexpr int kTypeId = 1;
    static constexpr uint64_t kBufGroup = 2;
    static constexpr size_t kBufSize = __FAAS_MESSAGE_SIZE * 4;

    explicit MessageConnection(Engine* engine, int sockfd);
    ~MessageConnection();

    uint16_t func_id() const { return func_id_; }
    uint16_t client_id() const { return client_id_; }
    bool handshake_done() const { return handshake_done_; }
    bool is_launcher_connection() const { return client_id_ == 0; }
    bool is_func_worker_connection() const { return client_id_ > 0; }

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    // Must be thread-safe
    void WriteMessage(const protocol::Message& message);

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    Engine* engine_;
    IOWorker* io_worker_;
    State state_;
    uint16_t func_id_;
    uint16_t client_id_;
    bool handshake_done_;

    int sockfd_;
    int in_fifo_fd_;
    int out_fifo_fd_;
    std::atomic<int> pipe_for_write_fd_;

    std::string log_header_;

    utils::AppendableBuffer message_buffer_;
    protocol::Message handshake_response_;
    utils::AppendableBuffer write_message_buffer_;

    absl::Mutex write_message_mu_;
    absl::InlinedVector<protocol::Message, 16>
        pending_messages_ ABSL_GUARDED_BY(write_message_mu_);

    void RecvHandshakeMessage();
    void SendPendingMessages();
    bool OnRecvSockData(int status, std::span<const char> data);
    bool OnRecvData(int status, std::span<const char> data);
    void OnFdClosed();

    bool WriteMessageWithFifo(const protocol::Message& message);

    DISALLOW_COPY_AND_ASSIGN(MessageConnection);
};

}  // namespace engine
}  // namespace faas
