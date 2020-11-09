#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "log/core.h"

namespace faas {
namespace engine {

class Engine;

class SharedLogEngine {
public:
    explicit SharedLogEngine(Engine* engine);
    ~SharedLogEngine();

    const log::Core* log_core() const { return &log_core_; }

    void OnSequencerMessage(int seqnum, std::span<const char> data);
    void OnMessageFromOtherEngine(const protocol::Message& message);
    void OnMessageFromFuncWorker(const protocol::Message& message);
    void OnLogReplicated(std::pair<uint64_t, uint64_t> localid_range,
                         std::pair<uint64_t, uint64_t> seqnum_range);
    void OnLogDiscarded(uint64_t localid);
    void AppendBackupLog(uint16_t view_id, uint16_t backup_node_id,
                         uint64_t log_localid, uint32_t log_tag,
                         std::span<const char> data);
    void SendSequencerMessage(std::span<const char> data);

private:
    Engine* engine_;
    log::Core log_core_;

    DISALLOW_COPY_AND_ASSIGN(SharedLogEngine);
};

}  // namespace engine
}  // namespace faas
