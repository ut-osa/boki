#pragma once

#include "base/common.h"
#include "utils/socket.h"
#include "utils/appendable_buffer.h"
#include "utils/round_robin_set.h"
#include "server/io_worker.h"

namespace faas {
namespace server {

class EgressHub : public ConnectionBase {
public:
    EgressHub(int type, const struct sockaddr_in* addr, size_t num_conn);
    virtual ~EgressHub();

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    void set_log_header(std::string_view log_header) {
        log_header_ = std::string(log_header);
    }

    typedef std::function<void(std::string* /* handshake */)>
            HandshakeMessageCallback;
    void SetHandshakeMessageCallback(HandshakeMessageCallback cb);

    void SendMessage(std::span<const char> message);
    void SendMessage(std::span<const char> part1, std::span<const char> part2);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    IOWorker* io_worker_;
    State state_;
    struct sockaddr_in addr_;

    std::vector<int> sockfds_;
    utils::RoundRobinSet</* sockfd */ int> connections_for_pick_;

    HandshakeMessageCallback  handshake_message_cb_;

    std::string log_header_;
    utils::AppendableBuffer write_buffer_;
    bool send_fn_scheduled_;

    void OnSocketConnected(int sockfd, int status);
    void SocketReady(int sockfd);
    void RemoveSocket(int sockfd);
    void ScheduleSendFunction();
    void SendPendingMessages();

    DISALLOW_COPY_AND_ASSIGN(EgressHub);
};

}  // namespace server
}  // namespace faas
