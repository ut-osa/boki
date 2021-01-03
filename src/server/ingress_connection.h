#pragma once

#include "base/common.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"

namespace faas {
namespace server {

class IngressConnection : public ConnectionBase {
public:
    IngressConnection(int type, int sockfd, size_t msghdr_size);
    virtual ~IngressConnection();

    static constexpr uint16_t kIngressBufGroup = 254;
    static constexpr size_t   kBufSize         = 65536;

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    void set_log_header(std::string_view log_header) {
        log_header_ = std::string(log_header);
    }

    typedef std::function<size_t(std::span<const char> /* header */)>
            MessageFullSizeCallback;
    void SetMessageFullSizeCallback(MessageFullSizeCallback cb);

    typedef std::function<void(std::span<const char> /* message */)>
            NewMessageCallback;
    void SetNewMessageCallback(NewMessageCallback cb);

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    IOWorker* io_worker_;
    State state_;
    int sockfd_;
    size_t msghdr_size_;

    MessageFullSizeCallback  message_full_size_cb_;
    NewMessageCallback       new_message_cb_;

    std::string log_header_;
    utils::AppendableBuffer read_buffer_;

    void ProcessMessages();
    bool OnRecvData(int status, std::span<const char> data);

    DISALLOW_COPY_AND_ASSIGN(IngressConnection);
};

}  // namespace server
}  // namespace faas
