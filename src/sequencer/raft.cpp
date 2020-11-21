#include "sequencer/raft.h"

#define HLOG(l) LOG(l) << "Raft: "
#define HVLOG(l) VLOG(l) << "Raft: "

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
    all_nodes_.clear();
    for (const auto& entry : all_nodes) {
        all_nodes_[entry.first] = std::string(entry.second);
    }
    DCHECK(all_nodes_.contains(id_));

    RAFT_DCHECK_OK(raft_uv_tcp_init(&transport_, uv_loop));
    RAFT_DCHECK_OK(raft_uv_init(&raft_io_, uv_loop, data_dir_.c_str(), &transport_));
    RAFT_DCHECK_OK(raft_init(&raft_, &raft_io_, &raft_fsm_, id_, listen_address_.c_str()));

    struct raft_configuration configuration;
    for (const auto& entry : all_nodes_) {
        RAFT_DCHECK_OK(raft_configuration_add(
            &configuration, /* id= */ entry.first,
            /* address= */ entry.second.c_str(), RAFT_VOTER));
    }
    RAFT_DCHECK_OK(raft_bootstrap(&raft_, &configuration));
    raft_configuration_close(&configuration);

    // TODO: investigate these parameters
    raft_set_snapshot_threshold(&raft_, 64);
    raft_set_snapshot_trailing(&raft_, 16);
    raft_set_pre_vote(&raft_, true);

    HLOG(INFO) << fmt::format("Raft voter started with id {}", id_);
    RAFT_DCHECK_OK(raft_start(&raft_));
    state_ = kRunning;
}

bool Raft::IsLeader() const {
    if (state_ != kRunning) {
        HLOG(WARNING) << "Not in running state";
        return false;
    } else {
        return raft_.state == RAFT_LEADER;
    }
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
    buf.len = payload.size();
    buf.base = raft_malloc(buf.len);
    memcpy(buf.base, payload.data(), buf.len);
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

void Raft::ScheduleClose() {
    if (state_ == kClosing) {
        HLOG(WARNING) << "Already scheduled for closing";
        return;
    }
    DCHECK(state_ == kRunning);
    state_ = kClosing;
    if (IsLeader()) {
        transfer_req_.data = this;
        RAFT_DCHECK_OK(raft_transfer(&raft_, &transfer_req_, 0,
                                     &Raft::TransferCallbackWrapper));
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
        HLOG(WARNING) << "Apply failed with error: " << raft_strerror(status);
    }
    apply_cbs_[req](status == 0);
    apply_cbs_.erase(req);
    apply_req_pool_.Return(req);
}

void Raft::OnLeadershipTransferred() {
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
    std::span<const char> payload(reinterpret_cast<char*>(buf->base), buf->len);
    if (!self->fsm_apply_cb_(payload)) {
        return RAFT_INVALID;
    }
    *result = nullptr;
    return 0;
}

int Raft::FsmSnapshotCallbackWrapper(struct raft_fsm* fsm,
                                     struct raft_buffer* bufs[], unsigned* n_bufs) {
    Raft* self = reinterpret_cast<Raft*>(fsm->data);
    std::string data;
    if (!self->fsm_snapshot_cb_(&data)) {
        return RAFT_INVALID;
    }
    *n_bufs = 1;
    *bufs = (struct raft_buffer*) raft_malloc(sizeof(struct raft_buffer*));
    if (*bufs == NULL) {
        return RAFT_NOMEM;
    }
    (*bufs)[0].len = data.size();
    (*bufs)[0].base = raft_malloc(data.size());
    if ((*bufs)[0].base == NULL) {
        raft_free(*bufs);
        return RAFT_NOMEM;
    }
    memcpy((*bufs)[0].base, data.data(), data.size());
    return 0;
}

int Raft::FsmRestoreCallbackWrapper(struct raft_fsm* fsm, struct raft_buffer* buf) {
    auto cleanup = gsl::finally([buf] { raft_free(buf->base); });
    Raft* self = reinterpret_cast<Raft*>(fsm->data);
    std::span<const char> payload(reinterpret_cast<char*>(buf->base), buf->len);
    if (!self->fsm_restore_cb_(payload)) {
        return RAFT_INVALID;
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

}  // namespace sequencer
}  // namespace faas
