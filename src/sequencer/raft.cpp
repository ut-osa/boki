#include "sequencer/raft.h"

#include "sequencer/flags.h"

#define log_header_ "Raft: "

#define RAFT_CHECK_OK(RAFT_CALL)                            \
    do {                                                    \
        int ret = RAFT_CALL;                                \
        LOG_IF(FATAL, ret != 0) << "libraft call failed: "  \
                                << raft_strerror(ret);      \
    } while (0)

#define RAFT_DCHECK_OK(RAFT_CALL)                           \
    do {                                                    \
        int ret = RAFT_CALL;                                \
        DLOG_IF(FATAL, ret != 0) << "libraft call failed: " \
                                 << raft_strerror(ret);     \
    } while (0)

namespace faas {
namespace sequencer {

Raft::Raft(uint64_t id)
    : state_(kCreated),
      id_(id),
      fsm_apply_cb_([] (std::span<const char>) { return false; }),
      fsm_restore_cb_([] (std::span<const char>) { return false; }),
      fsm_snapshot_cb_([] (std::string*) { return false; }) {
    raft_.data = this;
    raft_fsm_.version = 1;
    raft_fsm_.data = this;
    raft_fsm_.apply = &Raft::FsmApplyCallbackWrapper;
    raft_fsm_.snapshot = &Raft::FsmSnapshotCallbackWrapper;
    raft_fsm_.restore = &Raft::FsmRestoreCallbackWrapper;
    transfer_req_.data = nullptr;
}

Raft::~Raft() {
    DCHECK(state_ == kCreated || state_ == kClosed);
}

void Raft::SetFsmApplyCallback(FsmApplyCallback cb) {
    fsm_apply_cb_ = cb;
}

void Raft::SetFsmRestoreCallback(FsmRestoreCallback cb) {
    fsm_restore_cb_ = cb;
}

void Raft::SetFsmSnapshotCallback(FsmSnapshotCallback cb) {
    fsm_snapshot_cb_ = cb;
}

void Raft::Start(uv_loop_t* uv_loop, std::string_view listen_address,
                 std::string_view data_dir, const NodeVec& all_nodes) {
    DCHECK(state_ == kCreated);
    listen_address_ = std::string(listen_address);
    data_dir_ = std::string(data_dir);

    RAFT_DCHECK_OK(raft_uv_tcp_init(&transport_, uv_loop));
    RAFT_DCHECK_OK(raft_uv_init(&raft_io_, uv_loop, data_dir_.c_str(), &transport_));
    RAFT_DCHECK_OK(raft_init(&raft_, &raft_io_, &raft_fsm_, id_, listen_address_.c_str()));

    if (absl::GetFlag(FLAGS_enable_raft_tracer)) {
        tracer_.impl = this;
        tracer_.emit = &Raft::TraceEmitWrapper;
        raft_.tracer = &tracer_;
    }

    struct raft_configuration configuration;
    raft_configuration_init(&configuration);
    for (const auto& entry : all_nodes) {
        RAFT_DCHECK_OK(raft_configuration_add(
            &configuration, /* id= */ entry.first,
            /* address= */ entry.second.c_str(), RAFT_VOTER));
    }
    RAFT_DCHECK_OK(raft_bootstrap(&raft_, &configuration));
    raft_configuration_close(&configuration);

    raft_set_election_timeout(&raft_, absl::GetFlag(FLAGS_raft_election_timeout_ms));
    raft_set_heartbeat_timeout(&raft_, absl::GetFlag(FLAGS_raft_heartbeat_timeout_ms));
    if (absl::GetFlag(FLAGS_raft_enable_snapshot)) {
        raft_set_snapshot_threshold(&raft_, absl::GetFlag(FLAGS_raft_snapshot_threshold));
        raft_set_snapshot_trailing(&raft_, absl::GetFlag(FLAGS_raft_snapshot_trailing));
    } else {
        // Set to a very large value to essentially disable snapshot
        raft_set_snapshot_threshold(&raft_, std::numeric_limits<unsigned>::max());
    }
    raft_set_pre_vote(&raft_, absl::GetFlag(FLAGS_raft_pre_vote));

    HLOG(INFO) << fmt::format("Raft voter started with id {}", id_);
    RAFT_DCHECK_OK(raft_start(&raft_));
    state_ = kRunning;
}

uint64_t Raft::GetLeader() {
    if (state_ != kRunning) {
        HLOG(WARNING) << "Not in running state";
        return 0;
    } else {
        raft_id leader_id;
        const char* addr;
        raft_leader(&raft_, &leader_id, &addr);
        return uint64_t{leader_id};
    }
}

bool Raft::IsLeader() {
    if (state_ != kRunning) {
        HLOG(WARNING) << "Not in running state";
        return false;
    }
    return raft_state(&raft_) == RAFT_LEADER;
}

void Raft::Apply(std::span<const char> payload, ApplyCallback cb) {
    if (state_ != kRunning) {
        HLOG(WARNING) << "Not in running state";
        cb(false);
        return;
    } else if (!IsLeader()) {
        HLOG(WARNING) << "Only leader should call Apply";
        cb(false);
        return;
    }
    struct raft_apply* req = apply_req_pool_.Get();
    req->data = this;
    struct raft_buffer buf;
    if (!EncodeToRaftBuffer(payload, &buf)) {
        HLOG(FATAL) << "EncodeToRaftBuffer failed";
    }
    int ret = raft_apply(&raft_, req, &buf, 1, Raft::ApplyCallbackWrapper);
    if (ret != 0) {
        HLOG(ERROR) << "raft_apply failed with error: " << raft_strerror(ret);
        cb(false);
        raft_free(buf.base);
        apply_req_pool_.Return(req);
        return;
    }
    apply_cbs_[req] = cb;
}

bool Raft::GiveUpLeadership(uint64_t next_leader) {
    if (!IsLeader() || transfer_req_.data != nullptr) {
        return false;
    }
    if (next_leader == 0) {
        HLOG(INFO) << "Give up leadership, the new leader will be selected automatically";
    } else {
        HLOG(INFO) << fmt::format("Transfer leadership to voter {}", next_leader);
    }
    transfer_req_.data = this;
    RAFT_DCHECK_OK(raft_transfer(&raft_, &transfer_req_, next_leader,
                                 &Raft::TransferCallbackWrapper));
    return true;
}

void Raft::ScheduleClose() {
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    state_ = kClosing;
    if (IsLeader()) {
        GiveUpLeadership();
    } else {
        raft_close(&raft_, &Raft::CloseCallbackWrapper);
    }
}

void Raft::OnApplyFinished(struct raft_apply* req, int status) {
    if (!apply_cbs_.contains(req)) {
        HLOG(ERROR) << "Cannot find this apply request!";
        return;
    }
    if (status != 0) {
        if (status == RAFT_LEADERSHIPLOST) {
            HLOG(INFO) << "Lost leadership while applying new record";
        } else {
            HLOG(WARNING) << "Apply failed with error: " << raft_strerror(status);
        }
    }
    apply_cbs_[req](status == 0);
    apply_cbs_.erase(req);
    apply_req_pool_.Return(req);
}

void Raft::OnLeadershipTransferred() {
    HLOG(INFO) << "Leadership transferred";
    transfer_req_.data = nullptr;
    if (state_ == kClosing) {
        raft_close(&raft_, &Raft::CloseCallbackWrapper);
    }
}

void Raft::OnRaftClosed() {
    raft_uv_close(&raft_io_);
    raft_uv_tcp_close(&transport_);
    state_ = kClosed;
}

int Raft::FsmApplyCallbackWrapper(struct raft_fsm* fsm,
                                  const struct raft_buffer* buf, void** result) {
    Raft* self = reinterpret_cast<Raft*>(fsm->data);
    if (!self->fsm_apply_cb_(DecodeFromRaftBuffer(buf))) {
        HLOG(FATAL) << "FsmApply failed";
    }
    *result = nullptr;
    return 0;
}

int Raft::FsmSnapshotCallbackWrapper(struct raft_fsm* fsm,
                                     struct raft_buffer* bufs[], unsigned* n_bufs) {
    Raft* self = reinterpret_cast<Raft*>(fsm->data);
    std::string data;
    if (!self->fsm_snapshot_cb_(&data)) {
        HLOG(FATAL) << "FsmSnapshot failed";
    }
    *n_bufs = 1;
    *bufs = (struct raft_buffer*) raft_malloc(sizeof(struct raft_buffer*));
    if (*bufs == NULL) {
        HLOG(FATAL) << "raft_malloc failed!";
    }
    (*bufs)[0].len = data.size();
    (*bufs)[0].base = raft_malloc(data.size());
    if ((*bufs)[0].base == NULL) {
        HLOG(FATAL) << "raft_malloc failed!";
    }
    memcpy((*bufs)[0].base, data.data(), data.size());
    return 0;
}

int Raft::FsmRestoreCallbackWrapper(struct raft_fsm* fsm, struct raft_buffer* buf) {
    auto cleanup = gsl::finally([buf] { raft_free(buf->base); });
    Raft* self = reinterpret_cast<Raft*>(fsm->data);
    std::span<const char> payload(reinterpret_cast<char*>(buf->base), buf->len);
    if (!self->fsm_restore_cb_(payload)) {
        HLOG(FATAL) << "FsmRestore failed";
    }
    return 0;
}

void Raft::ApplyCallbackWrapper(struct raft_apply* req, int status, void* result) {
    Raft* self = reinterpret_cast<Raft*>(req->data);
    DCHECK(result == nullptr);
    self->OnApplyFinished(req, status);
}

void Raft::TransferCallbackWrapper(struct raft_transfer* req) {
    Raft* self = reinterpret_cast<Raft*>(req->data);
    self->OnLeadershipTransferred();
}

void Raft::CloseCallbackWrapper(struct raft* raft) {
    Raft* self = reinterpret_cast<Raft*>(raft->data);
    self->OnRaftClosed();
}

void Raft::TraceEmitWrapper(struct raft_tracer* tracer,
                            const char* file, int line, const char* msg) {
    LOG(INFO) << fmt::format("RaftTracer[{}:{}]: {}", file, line, msg);
}

bool Raft::EncodeToRaftBuffer(std::span<const char> data, struct raft_buffer* buf) {
    size_t data_size = data.size();
    size_t buf_len = sizeof(size_t) + data_size;
    // Round up to multiply of 8 bytes
    if (buf_len % 8 != 0) {
        buf_len = (buf_len / 8 + 1) * 8;
    }
    char* ptr = reinterpret_cast<char*>(raft_malloc(buf_len));
    if (ptr == nullptr) {
        HLOG(ERROR) << "raft_malloc failed";
        return false;
    }
    memcpy(ptr, &data_size, sizeof(size_t));
    memcpy(ptr + sizeof(size_t), data.data(), data_size);
    buf->base = ptr;
    buf->len = buf_len;
    return true;
}

std::span<const char> Raft::DecodeFromRaftBuffer(const struct raft_buffer* buf) {
    DCHECK(buf->len >= sizeof(size_t));
    size_t data_size;
    memcpy(&data_size, buf->base, sizeof(size_t));
    DCHECK(buf->len >= sizeof(size_t) + data_size);
    const char* data_base = reinterpret_cast<const char*>(buf->base) + sizeof(size_t);
    return std::span<const char>(data_base, data_size);
}

}  // namespace sequencer
}  // namespace faas
