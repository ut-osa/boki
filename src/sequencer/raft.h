#pragma once

#include "base/common.h"
#include "utils/object_pool.h"

#include <raft.h>
#include <raft/uv.h>

namespace faas {
namespace sequencer {

class Raft {
public:
    explicit Raft(uint64_t id);
    ~Raft();

    typedef std::function<bool(std::span<const char> /* payload */)> FsmApplyCallback;
    void SetFsmApplyCallback(FsmApplyCallback cb);

    typedef std::function<bool(std::span<const char> /* payload */)> FsmRestoreCallback;
    void SetFsmRestoreCallback(FsmRestoreCallback cb);

    typedef std::function<bool(std::string* /* data */)> FsmSnapshotCallback;
    void SetFsmSnapshotCallback(FsmSnapshotCallback cb);

    typedef std::vector<std::pair<uint64_t, std::string_view>> NodeVec;
    void Start(uv_loop_t* uv_loop, std::string_view listen_address,
               std::string_view data_dir, const NodeVec& all_nodes);
    

    bool IsLeader() const;

    typedef std::function<void(bool /* success */)> ApplyCallback;
    void Apply(std::span<const char> payload, ApplyCallback cb);

    void ScheduleClose();

private:
    enum State { kCreated, kRunning, kClosing, kClosed };

    State state_;
    uint64_t id_;

    FsmApplyCallback    fsm_apply_cb_;
    FsmRestoreCallback  fsm_restore_cb_;
    FsmSnapshotCallback fsm_snapshot_cb_;

    struct raft              raft_;
    struct raft_io           raft_io_;
    struct raft_fsm          raft_fsm_;
    struct raft_uv_transport transport_;
    struct raft_transfer     transfer_req_;

    std::string listen_address_;
    std::string data_dir_;
    absl::flat_hash_map<uint64_t, std::string> all_nodes_;

    utils::SimpleObjectPool<struct raft_apply> apply_req_pool_;
    absl::flat_hash_map<struct raft_apply*, ApplyCallback> apply_cbs_;

    void OnApplyFinished(struct raft_apply* req, int status);
    void OnLeadershipTransferred();
    void OnRaftClosed();

    static int FsmApplyCallbackWrapper(struct raft_fsm* fsm,
                                       const struct raft_buffer* buf, void** result);
    static int FsmSnapshotCallbackWrapper(struct raft_fsm* fsm,
                                          struct raft_buffer* bufs[], unsigned* n_bufs);
    static int FsmRestoreCallbackWrapper(struct raft_fsm* fsm, struct raft_buffer* buf);
    static void ApplyCallbackWrapper(struct raft_apply* req, int status, void* result);
    static void TransferCallbackWrapper(struct raft_transfer* req);
    static void CloseCallbackWrapper(struct raft* raft);

    DISALLOW_COPY_AND_ASSIGN(Raft);
};

}  // namespace sequencer
}  // namespace faas
