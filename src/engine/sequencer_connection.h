#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "server/constants.h"
#include "server/io_worker.h"

namespace faas {
namespace engine {

class Engine;
class SLogEngine;

class SequencerConnection final : public server::ConnectionBase {
public:
    static constexpr size_t kBufSize = 65536;

    static int type_id(uint16_t sequencer_id) {
        return kSequencerConnectionTypeId + sequencer_id;
    }

    SequencerConnection(Engine* engine, SLogEngine* slog_engine,
                        uint16_t sequencer_id, int sockfd);
    ~SequencerConnection();

    uint16_t sequencer_id() const { return sequencer_id_; }

    void Start(server::IOWorker* io_worker) override;
    void ScheduleClose() override;

    void SendMessage(const protocol::SequencerMessage& message,
                     std::span<const char> payload);

private:
    enum State { kCreated, kHandshake, kRunning, kClosing, kClosed };

    Engine* engine_;
    SLogEngine* slog_engine_;
    server::IOWorker* io_worker_;
    State state_;
    uint16_t sequencer_id_;
    int sockfd_;

    std::string log_header_;

    protocol::SequencerMessage handshake_message_;
    utils::AppendableBuffer read_buffer_;

    void ProcessSequencerMessages();
    bool OnRecvData(int status, std::span<const char> data);

    DISALLOW_COPY_AND_ASSIGN(SequencerConnection);
};

}  // namespace engine
}  // namespace faas
