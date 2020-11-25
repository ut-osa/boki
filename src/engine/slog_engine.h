#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/sequencer_config.h"
#include "utils/object_pool.h"
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

    std::string_view GetNodeAddr(uint16_t node_id);

private:
    Engine* engine_;
    const SequencerConfig* sequencer_config_;

    absl::Mutex mu_;

    log::EngineCore core_ ABSL_GUARDED_BY(mu_);
    std::unique_ptr<log::StorageInterface> storage_;

    enum LogOpType : uint16_t { kAppend, kRead, kTrim };

    struct LogOp {
        uint64_t id;  // Lower 8-bit stores type
        uint16_t client_id;
        uint64_t client_data;
        uint32_t log_tag;
        uint64_t log_seqnum;
    };

    utils::ThreadSafeObjectPool<LogOp> log_op_pool_;
    std::atomic<uint64_t> next_op_id_;

    absl::flat_hash_map</* localid */ uint64_t, LogOp*> append_ops_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* op_id */ uint64_t, LogOp*> read_ops_ ABSL_GUARDED_BY(mu_);

    inline LogOpType op_type(const LogOp* op) {
        return gsl::narrow_cast<LogOpType>(op->id & 0xff);
    }

    LogOp* AllocLogOp(LogOpType type, uint16_t client_id, uint64_t client_data);
    LogOp* GrabAppendLogOp(uint64_t localid) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);
    LogOp* GrabReadLogOp(uint64_t op_id) ABSL_EXCLUSIVE_LOCKS_REQUIRED(mu_);

    void SetupTimers();
    void LocalCutTimerTriggered();

    void HandleRemoteAppend(const protocol::Message& message);
    void HandleRemoteReadAt(const protocol::Message& message);

    void HandleLocalAppend(const protocol::Message& message);
    void HandleLocalReadNext(const protocol::Message& message);

    void ReadAtFinished(const protocol::Message& message);
    void LogPersisted(std::unique_ptr<log::LogEntry> log_entry);
    void LogDiscarded(std::unique_ptr<log::LogEntry> log_entry);

    void ReadLogFromStorage(uint64_t seqnum, uint64_t client_data,
                            protocol::Message* message);
    void AppendBackupLog(uint16_t view_id, uint16_t backup_node_id,
                         const log::LogEntry* log_entry);
    void SendSequencerMessage(const protocol::SequencerMessage& message,
                              std::span<const char> payload);
    void SendMessageToEngine(uint16_t node_id, const protocol::Message& message);
    void ScheduleLocalCut(int duration_us);

    DISALLOW_COPY_AND_ASSIGN(SLogEngine);
};

}  // namespace engine
}  // namespace faas
