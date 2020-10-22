#include "engine/io_uring.h"

#include <absl/flags/flag.h>

ABSL_FLAG(int, io_uring_cq_nr_wait, 1, "");
ABSL_FLAG(int, io_uring_cq_wait_timeout_us, 0, "");

#define ERRNO_LOGSTR(errno) fmt::format("{} [{}]", strerror(errno), errno)

namespace faas {
namespace engine {

std::atomic<int> IOUring::next_uring_id_{0};

IOUring::IOUring(int entries)
    : uring_id_(next_uring_id_.fetch_add(1)),
      next_op_id_(1),
      ev_loop_counter_(
          stat::Counter::StandardReportCallback(
              fmt::format("io_uring[{}] ev_loop", uring_id_))),
      wait_timeout_counter_(
          stat::Counter::StandardReportCallback(
              fmt::format("io_uring[{}] wait_timeout", uring_id_))),
      completed_ops_counter_(
          stat::Counter::StandardReportCallback(
              fmt::format("io_uring[{}] completed_ops", uring_id_))),
      completed_ops_stat_(
          stat::StatisticsCollector<int>::StandardReportCallback(
              fmt::format("io_uring[{}] completed_ops", uring_id_))) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    PCHECK(io_uring_queue_init_params(entries, &ring_, &params) == 0)
        << "io_uring init failed";
    CHECK((params.features & IORING_FEAT_FAST_POLL) != 0)
        << "IORING_FEAT_FAST_POLL not supported";
    memset(&cqe_wait_timeout, 0, sizeof(cqe_wait_timeout));
    int wait_timeout_us = absl::GetFlag(FLAGS_io_uring_cq_wait_timeout_us);
    if (wait_timeout_us != 0) {
        cqe_wait_timeout.tv_sec = wait_timeout_us / 1000000;
        cqe_wait_timeout.tv_nsec = int64_t{wait_timeout_us % 1000000} * 1000;
    }
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

bool IOUring::StartReadInternal(int fd, uint16_t buf_gid, uint16_t flags, ReadCallback cb) {
    if (close_cbs_.contains(fd)) {
        LOG(WARNING) << fmt::format("Fd {} already closed", fd);
        return false;
    }
    if (read_ops_.contains(fd)) {
        LOG(ERROR) << fmt::format("fd {} already registered read callback", fd);
        return false;
    }
    if (!buf_pools_.contains(buf_gid)) {
        LOG(ERROR) << fmt::format("Invalid buf_gid {}", buf_gid);
        return false;
    }
    std::span<char> buf;
    buf_pools_[buf_gid]->Get(&buf);
    Op* op = AllocReadOp(fd, buf_gid, buf, flags);
    read_cbs_[op->id] = cb;
    EnqueueOp(op);
    return true;
}

bool IOUring::StartRead(int fd, uint16_t buf_gid, ReadCallback cb) {
    return StartReadInternal(fd, buf_gid, kOpFlagRepeat, cb);
}

bool IOUring::StartRecv(int fd, uint16_t buf_gid, ReadCallback cb) {
    return StartReadInternal(fd, buf_gid, kOpFlagRepeat | kOpFlagUseRecv, cb);
}

bool IOUring::StopReadOrRecv(int fd) {
    if (read_ops_.contains(fd)) {
        Op* read_op = read_ops_[fd];
        read_op->flags |= kOpFlagCancelled;
        Op* op = AllocCancelOp(read_op->id);
        EnqueueOp(op);
        return true;
    } else {
        return false;
    }
}

bool IOUring::Write(int fd, std::span<const char> data, WriteCallback cb) {
    if (close_cbs_.contains(fd)) {
        LOG(WARNING) << fmt::format("Fd {} already closed", fd);
        return false;
    }
    Op* op = AllocWriteOp(fd, data);
    write_cbs_[op->id] = cb;
    EnqueueOp(op);
    return true;
}

bool IOUring::SendAll(int fd, std::span<const char> data, SendAllCallback cb) {
    if (close_cbs_.contains(fd)) {
        LOG(WARNING) << fmt::format("Fd {} already closed", fd);
        return false;
    }
    Op* op = AllocSendAllOp(fd, data);
    sendall_cbs_[op->id] = cb;
    if (last_send_op_.contains(fd)) {
        Op* last_op = last_send_op_[fd];
        DCHECK_EQ(op_type(last_op), kSendAll);
        DCHECK_EQ(last_op->next_op, kInvalidOpId);
        last_op->next_op = op->id;
    } else {
        EnqueueOp(op);
    }
    last_send_op_[fd] = op;
    return true;
}

bool IOUring::Close(int fd, CloseCallback cb) {
    if (close_cbs_.contains(fd)) {
        LOG(WARNING) << fmt::format("Fd {} already closed", fd);
        return false;
    }
    Op* op = AllocCloseOp(fd);
    close_cbs_[fd] = cb;
    if (last_send_op_.contains(fd)) {
        Op* last_op = last_send_op_[fd];
        DCHECK_EQ(op_type(last_op), kSendAll);
        DCHECK_EQ(last_op->next_op, kInvalidOpId);
        last_op->next_op = op->id;
    } else {
        EnqueueOp(op);
    }
    return true;
}

void IOUring::EventLoopRunOnce(int* inflight_ops) {
    struct io_uring_cqe* cqe = nullptr;
    int nr_wait = absl::GetFlag(FLAGS_io_uring_cq_nr_wait);
    if (absl::GetFlag(FLAGS_io_uring_cq_wait_timeout_us) == 0) {
        int ret = io_uring_submit_and_wait(&ring_, nr_wait);
        if (ret < 0) {
            LOG(FATAL) << "io_uring_submit_and_wait failed: " << ERRNO_LOGSTR(-ret);
        }
    } else {
        int ret = io_uring_wait_cqes(&ring_, &cqe, nr_wait, &cqe_wait_timeout, nullptr);
        if (ret < 0) {
            if (ret == -ETIME) {
                wait_timeout_counter_.Tick();
            } else {
                LOG(FATAL) << "io_uring_wait_cqes failed: " << ERRNO_LOGSTR(-ret);
            }
        }
    }
    ev_loop_counter_.Tick();
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
        completed_ops_counter_.Tick(count);
        completed_ops_stat_.AddSample(gsl::narrow_cast<int>(count));
    }
#if DCHECK_IS_ON()
    VLOG(1) << "Inflight ops:";
    for (const auto& item : ops_) {
        Op* op = item.second;
        DCHECK_EQ(op->id, item.first);
        VLOG(1) << fmt::format("id={}, type={}, fd={}", op->id, op_type(op), op->fd);
    }
#endif
    *inflight_ops = ops_.size();
}

#define ALLOC_OP(TYPE, OP_VAR)           \
    Op* OP_VAR = op_pool_.Get();         \
    uint64_t id = next_op_id_++;         \
    OP_VAR->id = (id << 8) + TYPE;       \
    OP_VAR->fd = kInvalidFd;             \
    OP_VAR->buf_gid = 0;                 \
    OP_VAR->flags = 0;                   \
    OP_VAR->buf = nullptr;               \
    OP_VAR->buf_len = 0;                 \
    OP_VAR->next_op = kInvalidOpId;      \
    ops_[op->id] = op

IOUring::Op* IOUring::AllocReadOp(int fd, uint16_t buf_gid, std::span<char> buf, uint16_t flags) {
    ALLOC_OP(kRead, op);
    op->fd = fd;
    op->buf_gid = buf_gid;
    op->flags = flags;
    op->buf = buf.data();
    op->buf_len = buf.size();
    RefFd(fd);
    read_ops_[fd] = op;
    return op;
}

IOUring::Op* IOUring::AllocWriteOp(int fd, std::span<const char> data) {
    ALLOC_OP(kWrite, op);
    op->fd = fd;
    op->buf = const_cast<char*>(data.data());
    op->buf_len = data.size();
    RefFd(fd);
    return op;
}

IOUring::Op* IOUring::AllocSendAllOp(int fd, std::span<const char> data) {
    ALLOC_OP(kSendAll, op);
    op->fd = fd;
    op->buf = const_cast<char*>(data.data());
    op->buf_len = data.size();
    RefFd(fd);
    return op;
}

IOUring::Op* IOUring::AllocCloseOp(int fd) {
    ALLOC_OP(kClose, op);
    op->fd = fd;
    RefFd(fd);
    return op;
}

IOUring::Op* IOUring::AllocCancelOp(uint64_t op_id) {
    ALLOC_OP(kCancel, op);
    op->next_op = op_id;
    return op;
}

#undef ALLOC_OP

void IOUring::EnqueueOp(Op* op) {
    VLOG(1) << fmt::format("EnqueueOp: id={}, type={}, fd={}", op->id, op_type(op), op->fd);
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    switch (op_type(op)) {
    case kRead:
        if (op->flags & kOpFlagUseRecv) {
            io_uring_prep_recv(sqe, op->fd, op->buf, op->buf_len, 0);
        } else {
            io_uring_prep_read(sqe, op->fd, op->buf, op->buf_len, 0);
        }
        break;
    case kWrite:
        io_uring_prep_write(sqe, op->fd, op->buf, op->buf_len, 0);
        break;
    case kSendAll:
        io_uring_prep_send(sqe, op->fd, op->buf, op->buf_len, 0);
        break;
    case kClose:
        io_uring_prep_close(sqe, op->fd);
        break;
    case kCancel:
        io_uring_prep_cancel(sqe, reinterpret_cast<void*>(op->next_op), 0);
        break;
    }
    sqe->user_data = op->id;
}

void IOUring::OnOpComplete(Op* op, struct io_uring_cqe* cqe) {
    int res = cqe->res;
    VLOG(1) << fmt::format("Op completed: id={}, type={}, fd={}, res={}",
                           op->id, op_type(op), op->fd, res);
    switch (op_type(op)) {
    case kRead:
        HandleReadOpComplete(op, res);
        break;
    case kWrite:
        HandleWriteOpComplete(op, res);
        break;
    case kSendAll:
        HandleSendallOpComplete(op, res);
        break;
    case kClose:
        // Close callback will be called in UnrefFd
        if (res < 0) {
            LOG(ERROR) << fmt::format("Failed to close fd {}: ", op->fd) << ERRNO_LOGSTR(-res);
        }
        break;
    case kCancel:
        if (res < 0 && res != -EALREADY) {
            LOG(ERROR) << fmt::format("Cancel Op failed with res={}", res);
        }
        break;
    }
    if (op->fd != kInvalidFd) {
        UnrefFd(op->fd);
    }
}

void IOUring::RefFd(int fd) {
    ref_counts_[fd]++;
}

void IOUring::UnrefFd(int fd) {
    if (--ref_counts_[fd] == 0) {
        ref_counts_.erase(fd);
    }
    if (close_cbs_.contains(fd) && !ref_counts_.contains(fd)) {
        close_cbs_[fd]();
        close_cbs_.erase(fd);
    }
}

void IOUring::HandleReadOpComplete(Op* op, int res) {
    DCHECK_EQ(op_type(op), kRead);
    read_ops_.erase(op->fd);
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
    if ((op->flags & kOpFlagRepeat) != 0
            && (op->flags & kOpFlagCancelled) == 0
            && repeat) {
        Op* new_op = AllocReadOp(op->fd, op->buf_gid,
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

void IOUring::HandleSendallOpComplete(Op* op, int res) {
    DCHECK_EQ(op_type(op), kSendAll);
    DCHECK(sendall_cbs_.contains(op->id));
    Op* new_op = nullptr;
    if (res >= 0) {
        size_t nwrite = gsl::narrow_cast<size_t>(res);
        if (nwrite == op->buf_len) {
            sendall_cbs_[op->id](0);
        } else {
            std::span<const char> remaining_data(op->buf + nwrite, op->buf_len - nwrite);
            new_op = AllocSendAllOp(op->fd, remaining_data);
            new_op->next_op = op->next_op;
            sendall_cbs_[new_op->id].swap(sendall_cbs_[op->id]);
        }
    } else {
        errno = -res;
        sendall_cbs_[op->id](-1);
    }
    sendall_cbs_.erase(op->id);
    if (new_op == nullptr) {
        if (op->next_op != kInvalidOpId) {
            DCHECK(ops_.contains(op->next_op));
            new_op = ops_[op->next_op];
        } else {
            DCHECK(last_send_op_.contains(op->fd));
            DCHECK_EQ(last_send_op_[op->fd], op);
            last_send_op_.erase(op->fd);
        }
    }
    if (new_op != nullptr) {
        EnqueueOp(new_op);
    }
}

}  // namespace engine
}  // namespace faas
