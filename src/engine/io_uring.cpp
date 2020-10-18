#include "engine/io_uring.h"

namespace faas {
namespace engine {

IOUring::IOUring(int entries) : next_op_id_(0) {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    PCHECK(io_uring_queue_init_params(entries, &ring_, &params) == 0)
        << "io_uring init failed";
    PCHECK((params.features & IORING_FEAT_FAST_POLL) != 0)
        << "IORING_FEAT_FAST_POLL not supported";
    struct io_uring_probe* probe = io_uring_get_probe_ring(&ring_);
    PCHECK(probe != nullptr && io_uring_opcode_supported(probe, IORING_OP_PROVIDE_BUFFERS))
        << "Buffer selection is not supported";
    free(probe);
}

IOUring::~IOUring() {
    CHECK(ops_.empty()) << "There are still inflight Ops";
    io_uring_queue_exit(&ring_);
}

void IOUring::PrepareBuffers(uint16_t gid, size_t buf_size) {
    if (buf_pools_.contains(gid)) {
        return;
    }
    buf_pools_[gid] = std::make_unique<utils::BufferPool>(fmt::format("IOUring-{}", gid), buf_size);
}

bool IOUring::StartRead(int fd, uint16_t buf_gid, bool repeat, ReadCallback cb) {
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
    Op* op = AllocReadOp(fd, buf_gid, buf, repeat);
    read_cbs_[op->id] = cb;
    EnqueueOp(op);
    return true;
}

bool IOUring::StopRead(int fd) {
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
    io_uring_submit_and_wait(&ring_, 1);
    struct io_uring_cqe* cqe;
    unsigned head;
    unsigned count = 0;
    io_uring_for_each_cqe(&ring_, head, cqe) {
        count++;
        uint64_t op_id = cqe->user_data;
        Op* op = ops_[op_id];
        ops_.erase(op_id);
        OnOpComplete(op, cqe);
        op_pool_.Return(op);
    }
    io_uring_cq_advance(&ring_, count);
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
    ops_[id] = op

IOUring::Op* IOUring::AllocReadOp(int fd, uint16_t buf_gid, std::span<char> buf, bool repeat) {
    ALLOC_OP(kRead, op);
    op->fd = fd;
    op->buf_gid = buf_gid;
    if (repeat) {
        op->flags |= kOpFlagRepeat;
    }
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
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    sqe->user_data = op->id;
    switch (op_type(op)) {
    case kRead:
        io_uring_prep_read(sqe, op->fd, op->buf, op->buf_len, 0);
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
}

void IOUring::OnOpComplete(Op* op, struct io_uring_cqe* cqe) {
    int res = cqe->res;
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
            LOG(ERROR) << fmt::format("Failed to close fd {}: {} [{}]",
                                      op->fd, strerror(-res), -res);
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
    bool cb_ret = false;
    if (res >= 0) {
        cb_ret = read_cbs_[op->id](0, std::span<const char>(op->buf, res));
    } else if (res != -ECANCELED) {
        errno = -res;
        cb_ret = read_cbs_[op->id](-1, std::span<const char>());
    }
    if ((op->flags & kOpFlagRepeat) != 0
            && (op->flags & kOpFlagCancelled) == 0
            && res != 0
            && cb_ret) {
        Op* new_op = AllocReadOp(op->fd, op->buf_gid, std::span<char>(op->buf, op->buf_len), true);
        read_cbs_[new_op->id] = std::move(read_cbs_[op->id]);
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
            sendall_cbs_[new_op->id] = std::move(sendall_cbs_[op->id]);
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
