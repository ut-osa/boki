#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "log/engine_core.h"
#include "log/storage.h"

namespace faas {
namespace engine {

class Engine;
class Timer;

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

    struct OngoingLogContext {
        uint16_t client_id;
        uint64_t client_data;
        uint32_t log_tag;
        uint64_t log_localid;
    };
    absl::flat_hash_map</* localid */ uint64_t, std::unique_ptr<OngoingLogContext>>
        ongoing_logs_ ABSL_GUARDED_BY(mu_);

    std::unique_ptr<OngoingLogContext> GrabOngoingLogContext(uint64_t localid)
        ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

    void SetupTimers();
    void LocalCutTimerTriggered();

    void HandleRemoteAppend(const protocol::Message& message);
    void HandleLocalAppend(const protocol::Message& message);

    void LogPersisted(std::unique_ptr<log::LogEntry> log_entry);
    void LogDiscarded(std::unique_ptr<log::LogEntry> log_entry);
    void AppendBackupLog(uint16_t view_id, uint16_t backup_node_id,
                         const log::LogEntry* log_entry);
    void SendSequencerMessage(const protocol::SequencerMessage& message,
                              std::span<const char> payload);
    void ScheduleLocalCut(int duration_us);

    DISALLOW_COPY_AND_ASSIGN(SLogEngine);
};

}  // namespace engine
}  // namespace faas
