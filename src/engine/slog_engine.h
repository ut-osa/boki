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

    uint16_t my_node_id() const;

    void OnNewExternalFuncCall(const protocol::FuncCall& func_call, uint32_t log_space);
    void OnNewInternalFuncCall(const protocol::FuncCall& func_call,
                               const protocol::FuncCall& parent_func_call);
    void OnFuncCallCompleted(const protocol::FuncCall& func_call);

    void OnSequencerMessage(const protocol::SequencerMessage& message,
                            std::span<const char> payload);
    void OnMessageFromOtherEngine(const protocol::Message& message);
    void OnMessageFromFuncWorker(const protocol::Message& message);

    std::string GetNodeAddr(uint16_t node_id);

private:
    Engine* engine_;
    const SequencerConfig* sequencer_config_;

    struct FuncCallContext {
        uint32_t log_space;
        uint32_t fsm_progress;
        uint64_t parent_call_id;
    };
    absl::Mutex func_ctx_mu_;
    absl::flat_hash_map</* full_call_id */ uint64_t, FuncCallContext>
        func_call_ctx_ ABSL_GUARDED_BY(func_ctx_mu_);

    absl::Mutex mu_;

    log::EngineCore core_ ABSL_GUARDED_BY(mu_);
    std::unique_ptr<log::StorageInterface> storage_;

    enum LogOpType : uint16_t {
        kAppend   = 0,
        kReadAt   = 1,
        kTrim     = 2,
        kReadNext = 3,
        kReadPrev = 4
    };

    static constexpr const char* kLopOpTypeStr[] = {
        "Append",
        "ReadAt",
        "Trim",
        "ReadNext",
        "ReadPrev"
    };

    struct LogOp {
        uint64_t id;  // Lower 8-bit stores type
        uint32_t log_space;
        uint32_t min_fsm_progress;
        uint16_t client_id;
        uint16_t src_node_id;
        uint64_t client_data;
        uint64_t log_tag;
        uint64_t log_seqnum;
        std::string log_data;
        protocol::FuncCall func_call;
        int remaining_retries;
        int64_t start_timestamp;
        uint16_t hop_times;
    };
    static constexpr int kMaxRetires = 3;

    utils::ThreadSafeObjectPool<LogOp> log_op_pool_;
    std::atomic<uint64_t> next_op_id_;

    absl::flat_hash_map</* localid */ uint64_t, LogOp*> append_ops_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* op_id */ uint64_t, LogOp*> remote_ops_ ABSL_GUARDED_BY(mu_);

    struct CompletedLogEntry {
        uint64_t localid;
        uint64_t seqnum;
        LogOp* append_op;
    };
    absl::InlinedVector<CompletedLogEntry, 8> completed_log_entries_ ABSL_GUARDED_BY(mu_);

    Timer* statecheck_timer_;

    FuncCallContext GetFuncContext(const protocol::FuncCall& func_call);
    void UpdateFuncFsmProgress(const protocol::FuncCall& func_call,
                               uint32_t fsm_progress);

    static inline LogOpType op_type(const LogOp* op) {
        return gsl::narrow_cast<LogOpType>(op->id & 0xff);
    }

    LogOp* AllocLogOp(LogOpType type, uint32_t log_space, uint32_t min_fsm_progress,
                      uint16_t client_id, uint64_t client_data);

    void SetupTimers();
    void LocalCutTimerTriggered();
    void StateCheckTimerTriggered();

    void HandleRemoteAppend(const protocol::Message& message);
    void HandleRemoteReplicate(const protocol::Message& message);
    void HandleRemoteReadAt(const protocol::Message& message);
    void HandleRemoteRead(const protocol::Message& message);

    void HandleLocalAppend(const protocol::Message& message);
    void HandleLocalRead(const protocol::Message& message, int direction);

    void RemoteOpFinished(const protocol::Message& response);
    void RemoteAppendFinished(const protocol::Message& message, LogOp* op);
    void RemoteReadAtFinished(const protocol::Message& message, LogOp* op);
    void RemoteReadFinished(const protocol::Message& message, LogOp* op);

    void LogPersisted(uint64_t localid, uint64_t seqnum);
    void LogDiscarded(uint64_t localid);

    void FinishLogOp(LogOp* op, protocol::Message* response);
    void ForwardLogOp(LogOp* op, uint16_t dst_node_id, protocol::Message* message);
    void NewAppendLogOp(LogOp* op, std::span<const char> data);
    void NewReadAtLogOp(LogOp* op, const log::Fsm::View* view, uint16_t primary_node_id);
    void NewReadLogOp(LogOp* op);

    void ReplicateLog(const log::Fsm::View* view, uint64_t tag, uint64_t localid,
                      std::span<const char> data);
    void ReadLogFromStorage(uint64_t seqnum, protocol::Message* response);
    void LogEntryCompleted(CompletedLogEntry entry, uint32_t fsm_progress);
    void RetryAppendOpIfDoable(LogOp* op);
    void RecordLogOpCompletion(LogOp* op);

    void SendFailedResponse(const protocol::Message& request,
                            protocol::SharedLogResultType result);
    void SendSequencerMessage(const protocol::SequencerMessage& message,
                              std::span<const char> payload);
    void SendMessageToEngine(uint16_t node_id, protocol::Message* message);
    void SendMessageToEngine(uint16_t src_node_id, uint16_t dst_node_id,
                             protocol::Message* message);
    void ScheduleLocalCut(int duration_us);
    void DoStateCheck();

    template<class KeyT>
    static LogOp* GrabLogOp(absl::flat_hash_map<KeyT, LogOp*>& op_map, KeyT key);

    DISALLOW_COPY_AND_ASSIGN(SLogEngine);
};

template<class KeyT>
SLogEngine::LogOp* SLogEngine::GrabLogOp(absl::flat_hash_map<KeyT, LogOp*>& op_map, KeyT key) {
    if (!op_map.contains(key)) {
        return nullptr;
    }
    LogOp* op = op_map[key];
    op_map.erase(key);
    return op;
}

}  // namespace engine
}  // namespace faas
