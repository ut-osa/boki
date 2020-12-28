#include "engine/io_uring.h"

#include "common/time.h"
#include "engine/flags.h"

#define ERRNO_LOGSTR(errno) fmt::format("{} [{}]", strerror(errno), errno)

namespace faas {
namespace engine {

std::atomic<int> IOUring::next_uring_id_{0};

IOUring::IOUring()
    : uring_id_(next_uring_id_.fetch_add(1, std::memory_order_relaxed)),
      log_header_(fmt::format("io_uring[{}]: ", uring_id_)),
      next_op_id_(1),
      ev_loop_counter_(stat::Counter::StandardReportCallback(
          fmt::format("io_uring[{}] ev_loop", uring_id_))),
      wait_timeout_counter_(stat::Counter::StandardReportCallback(
          fmt::format("io_uring[{}] wait_timeout", uring_id_))),
      completed_ops_counter_(stat::Counter::StandardReportCallback(
          fmt::format("io_uring[{}] completed_ops", uring_id_))),
      io_uring_enter_time_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          fmt::format("io_uring[{}] io_uring_enter_time", uring_id_))),
      completed_ops_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          fmt::format("io_uring[{}] completed_ops", uring_id_))),
      ev_loop_time_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          fmt::format("io_uring[{}] ev_loop_time", uring_id_))),
      average_op_time_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          fmt::format("io_uring[{}] average_op_time", uring_id_))) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    if (absl::GetFlag(FLAGS_io_uring_sqpoll)) {
        LOG(INFO) << "Enable IORING_SETUP_SQPOLL";
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = absl::GetFlag(FLAGS_io_uring_sq_thread_idle_ms);
    }
    int ret = io_uring_queue_init_params(
        absl::GetFlag(FLAGS_io_uring_entries), &ring_, &params);
    if (ret != 0) {
        LOG(FATAL) << "io_uring init failed: " << ERRNO_LOGSTR(-ret);
    }
    CHECK((params.features & IORING_FEAT_FAST_POLL) != 0)
        << "IORING_FEAT_FAST_POLL not supported";
    memset(&cqe_wait_timeout_, 0, sizeof(cqe_wait_timeout_));
    int wait_timeout_us = absl::GetFlag(FLAGS_io_uring_cq_wait_timeout_us);
    if (wait_timeout_us != 0) {
        cqe_wait_timeout_.tv_sec = wait_timeout_us / 1000000;
        cqe_wait_timeout_.tv_nsec = int64_t{wait_timeout_us % 1000000} * 1000;
    } else {
        CHECK_EQ(absl::GetFlag(FLAGS_io_uring_cq_nr_wait), 1)
            << "io_uring_cq_nr_wait should be set to 1 if timeout is 0";
    }
    int n_fd_slots = absl::GetFlag(FLAGS_io_uring_fd_slots);
    fds_.resize(n_fd_slots, -1);
    HLOG(INFO) << fmt::format("register {} fd slots", n_fd_slots);
    ret = io_uring_register_files(&ring_, fds_.data(), n_fd_slots);
    if (ret != 0) {
        LOG(FATAL) << "io_uring_register_files failed: " << ERRNO_LOGSTR(-ret);
    }
    read_ops_.resize(n_fd_slots, nullptr);
    last_send_op_.resize(n_fd_slots, nullptr);
    last_known_op_.resize(n_fd_slots, nullptr);
    close_cbs_.resize(n_fd_slots);
}

IOUring::~IOUring() {
    CHECK(ops_.empty()) << "There are still inflight Ops";
    io_uring_queue_exit(&ring_);
}

void IOUring::PrepareBuffers(uint16_t gid, size_t buf_size) {
    if (buf_pools_.contains(gid)) {
        return;
    }
    buf_pools_[gid] = std::make_unique<utils::BufferPool>(
        fmt::format("IOUring[{}]-{}", uring_id_, gid), buf_size);
}

bool IOUring::RegisterFd(int fd) {
    if (fd_indices_.contains(fd)) {
        LOG(ERROR) << fmt::format("fd {} already registered", fd);
        return false;
    }
    size_t index = 0;
    while (index < fds_.size() && fds_[index] != -1) {
        index++;
    }
    if (index == fds_.size()) {
        LOG(FATAL) << "No more fd slot, consider setting larger --io_uring_fd_slots";
    }
    int ret = io_uring_register_files_update(&ring_, index, &fd, 1);
    if (ret < 0) {
        LOG(FATAL) << "io_uring_register_files_update failed: " << ERRNO_LOGSTR(-ret);
    }
    fds_[index] = fd;
    fd_indices_[fd] = index;
    HLOG(INFO) << fmt::format("register fd {}, {} registered fds in total",
                              fd, fd_indices_.size());
    return true;
}

#define CHECK_AND_GET_FD_INDEX(FD_VAR, FD_IDX_VAR)                  \
    if (!fd_indices_.contains(FD_VAR)) {                            \
        LOG(ERROR) << fmt::format("fd {} not registered", FD_VAR);  \
        return false;                                               \
    }                                                               \
    size_t FD_IDX_VAR = fd_indices_[FD_VAR];                        \
    DCHECK_EQ(fds_[FD_IDX_VAR], FD_VAR)

#define CHECK_IF_CLOSED(FD_IDX_VAR)                                 \
    do {                                                            \
        Op* op = last_known_op_[FD_IDX_VAR];                        \
        if (op != nullptr && op_type(op) == kClose) {               \
            LOG(WARNING) << fmt::format("fd {} already closed",     \
                                        fds_[FD_IDX_VAR]);          \
            return false;                                           \
        }                                                           \
    } while (0)

bool IOUring::Connect(int fd, const struct sockaddr* addr, size_t addrlen, ConnectCallback cb) {
    CHECK_AND_GET_FD_INDEX(fd, fd_idx);
    CHECK_IF_CLOSED(fd_idx);
    Op* op = AllocConnectOp(fd_idx, addr, addrlen);
    connect_cbs_[op->id] = cb;
    EnqueueOp(op);
    last_known_op_[fd_idx] = op;
    return true;
}

bool IOUring::StartReadInternal(int fd, uint16_t buf_gid, uint16_t flags, ReadCallback cb) {
    CHECK_AND_GET_FD_INDEX(fd, fd_idx);
    CHECK_IF_CLOSED(fd_idx);
    if (read_ops_[fd_idx] != nullptr) {
        LOG(ERROR) << fmt::format("fd {} already registered read callback", fd);
        return false;
    }
    if (!buf_pools_.contains(buf_gid)) {
        LOG(ERROR) << fmt::format("Invalid buf_gid {}", buf_gid);
        return false;
    }
    std::span<char> buf;
    buf_pools_[buf_gid]->Get(&buf);
    Op* op = AllocReadOp(fd_idx, buf_gid, buf, flags);
    read_cbs_[op->id] = cb;
    EnqueueOp(op);
    last_known_op_[fd_idx] = op;
    return true;
}

bool IOUring::StartRead(int fd, uint16_t buf_gid, ReadCallback cb) {
    return StartReadInternal(fd, buf_gid, kOpFlagRepeat, cb);
}

bool IOUring::StartRecv(int fd, uint16_t buf_gid, ReadCallback cb) {
    return StartReadInternal(fd, buf_gid, kOpFlagRepeat | kOpFlagUseRecv, cb);
}

bool IOUring::StopReadOrRecv(int fd) {
    CHECK_AND_GET_FD_INDEX(fd, fd_idx);
    if (read_ops_[fd_idx] == nullptr) {
        return false;
    }
    Op* read_op = read_ops_[fd_idx];
    read_op->flags |= kOpFlagCancelled;
    Op* op = AllocCancelOp(read_op->id);
    EnqueueOp(op);
    last_known_op_[fd_idx] = op;
    return true;
}

bool IOUring::Write(int fd, std::span<const char> data, WriteCallback cb) {
    CHECK_AND_GET_FD_INDEX(fd, fd_idx);
    CHECK_IF_CLOSED(fd_idx);
    Op* op = AllocWriteOp(fd_idx, data);
    write_cbs_[op->id] = cb;
    EnqueueOp(op);
    last_known_op_[fd_idx] = op;
    return true;
}

bool IOUring::SendAll(int fd, std::span<const char> data, SendAllCallback cb) {
    CHECK_AND_GET_FD_INDEX(fd, fd_idx);
    CHECK_IF_CLOSED(fd_idx);
    Op* op = AllocSendAllOp(fd_idx, data);
    sendall_cbs_[op->id] = cb;
    if (last_send_op_[fd_idx] != nullptr) {
        Op* last_op = last_send_op_[fd_idx];
        DCHECK_EQ(op_type(last_op), kSendAll);
        DCHECK_EQ(last_op->next_op, kInvalidOpId);
        last_op->next_op = op->id;
    } else {
        EnqueueOp(op);
    }
    last_send_op_[fd_idx] = op;
    last_known_op_[fd_idx] = op;
    return true;
}

bool IOUring::Close(int fd, CloseCallback cb) {
    CHECK_AND_GET_FD_INDEX(fd, fd_idx);
    CHECK_IF_CLOSED(fd_idx);
    Op* op = AllocCloseOp(fd_idx);
    close_cbs_[fd_idx].swap(cb);
    if (last_known_op_[fd_idx] != nullptr) {
        Op* last_op = last_known_op_[fd_idx];
        DCHECK_EQ(last_op->next_op, kInvalidOpId);
        last_op->next_op = op->id;
    } else {
        EnqueueOp(op);
    }
    last_known_op_[fd_idx] = op;
    return true;
}

#undef CHECK_AND_GET_FD_INDEX
#undef CHECK_IF_CLOSED

void IOUring::EventLoopRunOnce(int* inflight_ops) {
    struct io_uring_cqe* cqe = nullptr;
    int nr_wait = absl::GetFlag(FLAGS_io_uring_cq_nr_wait);
    if (absl::GetFlag(FLAGS_io_uring_cq_wait_timeout_us) == 0) {
        int64_t start_timestamp = GetMonotonicNanoTimestamp();
        int ret = io_uring_submit_and_wait(&ring_, nr_wait);
        int64_t elasped_time = GetMonotonicNanoTimestamp() - start_timestamp;
        if (ret < 0) {
            LOG(FATAL) << "io_uring_submit_and_wait failed: " << ERRNO_LOGSTR(-ret);
        }
        io_uring_enter_time_stat_.AddSample(gsl::narrow_cast<int>(elasped_time));
    } else {
        int64_t start_timestamp = GetMonotonicNanoTimestamp();
        int ret = io_uring_wait_cqes(&ring_, &cqe, nr_wait, &cqe_wait_timeout_, nullptr);
        int64_t elasped_time = GetMonotonicNanoTimestamp() - start_timestamp;
        if (ret < 0) {
            if (ret == -ETIME) {
                wait_timeout_counter_.Tick();
            } else {
                LOG(FATAL) << "io_uring_wait_cqes failed: " << ERRNO_LOGSTR(-ret);
            }
        }
        io_uring_enter_time_stat_.AddSample(gsl::narrow_cast<int>(elasped_time));
    }
    ev_loop_counter_.Tick();
    int64_t start_timestamp = GetMonotonicNanoTimestamp();
    int count = 0;
    while (true) {
        cqe = nullptr;
        int ret = io_uring_peek_cqe(&ring_, &cqe);
        if (ret != 0) {
            if (ret == -ETIME) {
                continue;
            } else if (ret == -EAGAIN) {
                break;
            } else {
                LOG(FATAL) << "io_uring_peek_cqe failed: " << ERRNO_LOGSTR(-ret);
            }
        }
        uint64_t op_id = DCHECK_NOTNULL(cqe)->user_data;
        DCHECK(ops_.contains(op_id));
        Op* op = ops_[op_id];
        ops_.erase(op_id);
        OnOpComplete(op, cqe);
        op_pool_.Return(op);
        io_uring_cqe_seen(&ring_, cqe);
        count++;
    }
    if (count > 0) {
        int64_t elasped_time = GetMonotonicNanoTimestamp() - start_timestamp;
        ev_loop_time_stat_.AddSample(gsl::narrow_cast<int>(elasped_time));
        average_op_time_stat_.AddSample(gsl::narrow_cast<int>(elasped_time / count));
        completed_ops_counter_.Tick(count);
        completed_ops_stat_.AddSample(gsl::narrow_cast<int>(count));
    }
#if DCHECK_IS_ON()
    VLOG(2) << "Inflight ops:";
    for (const auto& item : ops_) {
        Op* op = item.second;
        DCHECK_EQ(op->id, item.first);
        VLOG(2) << fmt::format("id={}, type={}, fd={}", op->id, op_type(op), fds_[op->fd_idx]);
    }
#endif
    *inflight_ops = ops_.size();
}

#define ALLOC_OP(TYPE, OP_VAR)           \
    Op* OP_VAR = op_pool_.Get();         \
    uint64_t id = next_op_id_++;         \
    OP_VAR->id = (id << 8) + TYPE;       \
    OP_VAR->fd_idx = kInvalidFdIndex;    \
    OP_VAR->buf_gid = 0;                 \
    OP_VAR->flags = 0;                   \
    OP_VAR->buf = nullptr;               \
    OP_VAR->buf_len = 0;                 \
    OP_VAR->next_op = kInvalidOpId;      \
    ops_[op->id] = op

IOUring::Op* IOUring::AllocConnectOp(size_t fd_idx, const struct sockaddr* addr, size_t addrlen) {
    ALLOC_OP(kConnect, op);
    op->fd_idx = fd_idx;
    op->addr = addr;
    op->addrlen = addrlen;
    return op;
}

IOUring::Op* IOUring::AllocReadOp(size_t fd_idx, uint16_t buf_gid, std::span<char> buf,
                                  uint16_t flags) {
    ALLOC_OP(kRead, op);
    op->fd_idx = fd_idx;
    op->buf_gid = buf_gid;
    op->flags = flags;
    op->buf = buf.data();
    op->buf_len = buf.size();
    read_ops_[fd_idx] = op;
    return op;
}

IOUring::Op* IOUring::AllocWriteOp(size_t fd_idx, std::span<const char> data) {
    ALLOC_OP(kWrite, op);
    op->fd_idx = fd_idx;
    op->data = data.data();
    op->data_len = data.size();
    return op;
}

IOUring::Op* IOUring::AllocSendAllOp(size_t fd_idx, std::span<const char> data) {
    ALLOC_OP(kSendAll, op);
    op->fd_idx = fd_idx;
    op->data = data.data();
    op->data_len = data.size();
    return op;
}

IOUring::Op* IOUring::AllocCloseOp(size_t fd_idx) {
    ALLOC_OP(kClose, op);
    op->fd_idx = fd_idx;
    return op;
}

IOUring::Op* IOUring::AllocCancelOp(uint64_t op_id) {
    ALLOC_OP(kCancel, op);
    op->next_op = op_id;
    return op;
}

#undef ALLOC_OP

void IOUring::UnregisterFd(size_t fd_idx) {
    int fd = fds_[fd_idx];
    DCHECK(fd != -1);
    int value = -1;
    int ret = io_uring_register_files_update(&ring_, fd_idx, &value, 1);
    if (ret < 0) {
        LOG(FATAL) << "io_uring_register_files_update failed: " << ERRNO_LOGSTR(-ret);
    }
    DCHECK(read_ops_[fd_idx] == nullptr);
    DCHECK(last_send_op_[fd_idx] == nullptr);
    DCHECK(!close_cbs_[fd_idx]);
    fds_[fd_idx] = -1;
    fd_indices_.erase(fd);
    HLOG(INFO) << fmt::format("unregister fd {}, {} registered fds in total",
                              fd, fd_indices_.size());
}

void IOUring::EnqueueOp(Op* op) {
    VLOG(2) << fmt::format("EnqueueOp: id={}, type={}, fd={}",
                           op->id, op_type(op), fds_[op->fd_idx]);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    switch (op_type(op)) {
    case kConnect:
        io_uring_prep_connect(sqe, op->fd_idx, op->addr, op->addrlen);
        io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE | IOSQE_ASYNC);
        break;
    case kRead:
        if (op->flags & kOpFlagUseRecv) {
            io_uring_prep_recv(sqe, op->fd_idx, op->buf, op->buf_len, 0);
        } else {
            io_uring_prep_read(sqe, op->fd_idx, op->buf, op->buf_len, 0);
        }
        io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE | IOSQE_ASYNC);
        break;
    case kWrite:
        io_uring_prep_write(sqe, op->fd_idx, op->data, op->data_len, 0);
        io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
        break;
    case kSendAll:
        io_uring_prep_send(sqe, op->fd_idx, op->data, op->data_len, 0);
        io_uring_sqe_set_flags(sqe, IOSQE_FIXED_FILE);
        break;
    case kClose:
        io_uring_prep_close(sqe, fds_[op->fd_idx]);
        break;
    case kCancel:
        io_uring_prep_cancel(sqe, reinterpret_cast<void*>(op->next_op), 0);
        break;
    }
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(op->id));
}

void IOUring::OnOpComplete(Op* op, struct io_uring_cqe* cqe) {
    int res = cqe->res;
    VLOG(2) << fmt::format("Op completed: id={}, type={}, fd={}, res={}",
                           op->id, op_type(op), fds_[op->fd_idx], res);
    Op* next_op = nullptr;
    switch (op_type(op)) {
    case kConnect:
        HandleConnectComplete(op, res);
        break;
    case kRead:
        HandleReadOpComplete(op, res);
        break;
    case kWrite:
        HandleWriteOpComplete(op, res);
        break;
    case kSendAll:
        HandleSendallOpComplete(op, res, &next_op);
        break;
    case kClose:
        HandleCloseOpComplete(op, res);
        break;
    case kCancel:
        if (res < 0 && res != -EALREADY) {
            LOG(ERROR) << "Cancel Op failed: " << ERRNO_LOGSTR(-res);
        }
        break;
    }
    if (next_op == nullptr && op->next_op != kInvalidOpId) {
        DCHECK(ops_.contains(op->next_op));
        next_op = ops_[op->next_op];
    }
    if (op->fd_idx != kInvalidFdIndex && last_known_op_[op->fd_idx] == op) {
        last_known_op_[op->fd_idx] = nullptr;
    }
    if (next_op != nullptr) {
        EnqueueOp(next_op);
    }
}

void IOUring::HandleConnectComplete(Op* op, int res) {
    DCHECK_EQ(op_type(op), kConnect);
    DCHECK(connect_cbs_.contains(op->id));
    if (res >= 0) {
        connect_cbs_[op->id](0);
    } else {
        errno = -res;
        connect_cbs_[op->id](-1);
    }
    connect_cbs_.erase(op->id);
}

void IOUring::HandleReadOpComplete(Op* op, int res) {
    DCHECK_EQ(op_type(op), kRead);
    DCHECK(read_cbs_.contains(op->id));
    bool repeat = false;
    if (res >= 0) {
        repeat = read_cbs_[op->id](0, std::span<const char>(op->buf, res));
    } else if (res == -EAGAIN || res == -EINTR) {
        repeat = true;
    } else if (res != -ECANCELED) {
        errno = -res;
        repeat = read_cbs_[op->id](-1, std::span<const char>());
    }
    read_ops_[op->fd_idx] = nullptr;
    if ((op->flags & kOpFlagRepeat) != 0
            && (op->flags & kOpFlagCancelled) == 0
            && repeat) {
        Op* new_op = AllocReadOp(op->fd_idx, op->buf_gid,
                                 std::span<char>(op->buf, op->buf_len), op->flags);
        read_cbs_[new_op->id].swap(read_cbs_[op->id]);
        read_cbs_.erase(op->id);
        EnqueueOp(new_op);
    } else {
        read_cbs_.erase(op->id);
        DCHECK(buf_pools_.contains(op->buf_gid));
        buf_pools_[op->buf_gid]->Return(op->buf);
    }
}

void IOUring::HandleWriteOpComplete(Op* op, int res) {
    DCHECK_EQ(op_type(op), kWrite);
    DCHECK(write_cbs_.contains(op->id));
    if (res >= 0) {
        write_cbs_[op->id](0, res);
    } else {
        errno = -res;
        write_cbs_[op->id](-1, 0);
    }
    write_cbs_.erase(op->id);
}

void IOUring::HandleSendallOpComplete(Op* op, int res, Op** next_op) {
    DCHECK_EQ(op_type(op), kSendAll);
    DCHECK(sendall_cbs_.contains(op->id));
    size_t fd_idx = op->fd_idx;
    if (res >= 0) {
        size_t nwrite = gsl::narrow_cast<size_t>(res);
        if (nwrite == op->buf_len) {
            sendall_cbs_[op->id](0);
        } else {
            std::span<const char> remaining_data(op->buf + nwrite, op->buf_len - nwrite);
            Op* new_op = AllocSendAllOp(fd_idx, remaining_data);
            new_op->next_op = op->next_op;
            sendall_cbs_[new_op->id].swap(sendall_cbs_[op->id]);
            if (last_send_op_[fd_idx] == op) {
                last_send_op_[fd_idx] = new_op;
            }
            *next_op = new_op;
        }
    } else {
        errno = -res;
        sendall_cbs_[op->id](-1);
    }
    sendall_cbs_.erase(op->id);
    if (last_send_op_[fd_idx] == op) {
        last_send_op_[fd_idx] = nullptr;
    }
}

void IOUring::HandleCloseOpComplete(Op* op, int res) {
    DCHECK_EQ(op_type(op), kClose);
    size_t fd_idx = op->fd_idx;
    DCHECK_EQ(last_known_op_[fd_idx], op);
    if (res < 0) {
        LOG(FATAL) << fmt::format("Failed to close fd {}: ", fds_[fd_idx])
                   << ERRNO_LOGSTR(-res);
    }
    DCHECK(close_cbs_[fd_idx]);
    CloseCallback cb;
    cb.swap(close_cbs_[fd_idx]);
    cb();
    UnregisterFd(fd_idx);
}

}  // namespace engine
}  // namespace faas
