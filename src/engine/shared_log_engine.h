#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "log/engine_core.h"
#include "log/storage.h"

namespace faas {
namespace engine {

class Engine;

class SharedLogEngine {
public:
    explicit SharedLogEngine(Engine* engine);
    ~SharedLogEngine();

    void OnSequencerMessage(std::span<const char> data);
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

    DISALLOW_COPY_AND_ASSIGN(SharedLogEngine);
};

}  // namespace engine
}  // namespace faas
