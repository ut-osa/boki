#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "log/engine_core.h"
#include "log/storage.h"

namespace faas {
namespace engine {

class Engine;

class SLogEngine {
public:
    explicit SLogEngine(Engine* engine);
    ~SLogEngine();

    void OnSequencerMessage(const protocol::SequencerMessage& message,
                            std::span<const char> payload);
    void OnMessageFromOtherEngine(const protocol::Message& message);
    void OnMessageFromFuncWorker(const protocol::Message& message);

    std::string_view GetNodeAddr(uint16_t view_id, uint16_t node_id);

private:
    Engine* engine_;

    absl::Mutex mu_;

    log::EngineCore core_ ABSL_GUARDED_BY(mu_);
    std::unique_ptr<log::StorageInterface> storage_;

    void LogDiscarded(std::unique_ptr<log::LogEntry> log_entry);
    void AppendBackupLog(uint16_t view_id, uint16_t backup_node_id,
                         const log::LogEntry* log_entry);
    void SendSequencerMessage(std::span<const char> data);

    DISALLOW_COPY_AND_ASSIGN(SLogEngine);
};

}  // namespace engine
}  // namespace faas
