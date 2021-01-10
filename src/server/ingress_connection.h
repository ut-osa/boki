#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"

namespace faas {
namespace server {

class IngressConnection : public ConnectionBase {
public:
    IngressConnection(int type, int sockfd, size_t msghdr_size);
    virtual ~IngressConnection();

    static constexpr size_t kDefaultBufSize  = 65536;

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    void set_buffer_group(uint16_t buf_group, size_t buf_size) {
        buf_group_ = buf_group;
        buf_size_  = buf_size;
    }

    typedef std::function<size_t(std::span<const char> /* header */)>
            MessageFullSizeCallback;
    void SetMessageFullSizeCallback(MessageFullSizeCallback cb);

    typedef std::function<void(std::span<const char> /* message */)>
            NewMessageCallback;
    void SetNewMessageCallback(NewMessageCallback cb);

    static size_t GatewayMessageFullSizeCallback(std::span<const char> header);
    static NewMessageCallback BuildNewGatewayMessageCallback(
        std::function<void(const protocol::GatewayMessage&,
                           std::span<const char> /* payload */)> cb);

    static size_t SharedLogMessageFullSizeCallback(std::span<const char> header);
    static NewMessageCallback BuildNewSharedLogMessageCallback(
        std::function<void(const protocol::SharedLogMessage&,
                           std::span<const char> /* payload */)> cb);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    IOWorker* io_worker_;
    State state_;
    int sockfd_;
    size_t msghdr_size_;

    uint16_t buf_group_;
    size_t   buf_size_;

    MessageFullSizeCallback  message_full_size_cb_;
    NewMessageCallback       new_message_cb_;

    std::string log_header_;
    utils::AppendableBuffer read_buffer_;

    void ProcessMessages();
    bool OnRecvData(int status, std::span<const char> data);

    static std::string GetLogHeader(int type, int sockfd);

    DISALLOW_COPY_AND_ASSIGN(IngressConnection);
};

}  // namespace server
}  // namespace faas
