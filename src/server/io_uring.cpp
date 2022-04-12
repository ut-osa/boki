#include "server/io_uring.h"

#include "base/init.h"
#include "common/time.h"
#include "utils/bits.h"

ABSL_FLAG(size_t, io_uring_entries, 2048, "");
ABSL_FLAG(size_t, io_uring_fd_slots, 1024, "");
ABSL_FLAG(bool, io_uring_sqpoll, false, "");
ABSL_FLAG(uint32_t, io_uring_sq_thread_idle_ms, 1, "");
ABSL_FLAG(uint32_t, io_uring_cq_nr_wait, 1, "");
ABSL_FLAG(uint32_t, io_uring_cq_wait_timeout_us, 0, "");

#define ERRNO_LOGSTR(errno) fmt::format("{} [{}]", strerror(errno), errno)

namespace faas {
namespace server {

std::atomic<int> IOUring::next_uring_id_{0};

IOUring::IOUring()
    : uring_id_(next_uring_id_.fetch_add(1, std::memory_order_relaxed)),
      log_header_(fmt::format("io_uring[{}]: ", uring_id_)),
      next_op_id_(1),
      ev_loop_counter_(stat::Counter::VerboseLogReportCallback<2>(
          fmt::format("io_uring[{}] ev_loop", uring_id_))),
      wait_timeout_counter_(stat::Counter::VerboseLogReportCallback<2>(
          fmt::format("io_uring[{}] wait_timeout", uring_id_))),
      completed_ops_counter_(stat::Counter::VerboseLogReportCallback<2>(
          fmt::format("io_uring[{}] completed_ops", uring_id_))),
      io_uring_enter_time_stat_(stat::StatisticsCollector<int>::VerboseLogReportCallback<2>(
          fmt::format("io_uring[{}] io_uring_enter_time", uring_id_))),
      completed_ops_stat_(stat::StatisticsCollector<int>::VerboseLogReportCallback<2>(
          fmt::format("io_uring[{}] completed_ops", uring_id_))),
      ev_loop_time_stat_(stat::StatisticsCollector<int>::VerboseLogReportCallback<2>(
          fmt::format("io_uring[{}] ev_loop_time", uring_id_))),
      average_op_time_stat_(stat::StatisticsCollector<int>::VerboseLogReportCallback<2>(
          fmt::format("io_uring[{}] average_op_time", uring_id_))) {
    SetupUring();
    SetupFdSlots();
    base::ChainCleanupFn(absl::bind_front(&IOUring::CleanUpFn, this));
}

IOUring::~IOUring() {
    CHECK(ops_.empty()) << "There are still inflight Ops";
    io_uring_queue_exit(&ring_);
}

void IOUring::SetupUring() {
    struct io_uring_params params;
    memset(&params, 0, sizeof(params));
    if (absl::GetFlag(FLAGS_io_uring_sqpoll)) {
        LOG(INFO) << "Enable IORING_SETUP_SQPOLL";
        params.flags |= IORING_SETUP_SQPOLL;
        params.sq_thread_idle = absl::GetFlag(FLAGS_io_uring_sq_thread_idle_ms);
    }
    int ret = io_uring_queue_init_params(
        gsl::narrow_cast<uint32_t>(absl::GetFlag(FLAGS_io_uring_entries)),
        &ring_, &params);
    if (ret != 0) {
        LOG(FATAL) << "io_uring init failed: " << ERRNO_LOGSTR(-ret);
    }
    CHECK((params.features & IORING_FEAT_FAST_POLL) != 0)
        << "IORING_FEAT_FAST_POLL not supported";
    memset(&cqe_wait_timeout_, 0, sizeof(cqe_wait_timeout_));
    uint32_t wait_timeout_us = absl::GetFlag(FLAGS_io_uring_cq_wait_timeout_us);
    if (wait_timeout_us != 0) {
        cqe_wait_timeout_.tv_sec = wait_timeout_us / 1000000;
        cqe_wait_timeout_.tv_nsec = int64_t{wait_timeout_us % 1000000} * 1000;
    } else {
        CHECK_EQ(absl::GetFlag(FLAGS_io_uring_cq_nr_wait), 1U)
            << "io_uring_cq_nr_wait should be set to 1 if timeout is 0";
    }
}

void IOUring::SetupFdSlots() {
    size_t n_fd_slots = absl::GetFlag(FLAGS_io_uring_fd_slots);
    CHECK_GT(n_fd_slots, 0U);
    std::vector<int> tmp;
    tmp.resize(n_fd_slots, -1);
    int ret = io_uring_register_files(
        &ring_, tmp.data(), gsl::narrow_cast<uint32_t>(n_fd_slots));
    if (ret != 0) {
        LOG(FATAL) << "io_uring_register_files failed: " << ERRNO_LOGSTR(-ret);
    }
    HLOG_F(INFO, "register {} fd slots", n_fd_slots);
    fds_.resize(n_fd_slots);
    free_fd_slots_.reserve(n_fd_slots);
    for (size_t i = 0; i < n_fd_slots; i++) {
        fds_[i].fd = -1;
        free_fd_slots_.push_back(i);
    }
    absl::c_reverse(free_fd_slots_);
}

void IOUring::CleanUpFn() {
    fprintf(stderr, "Cleaning up io_uring[%d]\n", uring_id_);
    size_t n_fd_slots = fds_.size();
    for (size_t i = 0; i < n_fd_slots; i++) {
        int fd = fds_[i].fd;
        if (fd != -1 && close(fd) != 0) {
            fprintf(stderr, "Failed to close fd %d: %s\n", fd, strerror(errno));
        }
    }
    io_uring_queue_exit(&ring_);
}

void IOUring::PrepareBuffers(uint16_t gid, size_t buf_size) {
    if (buf_pools_.contains(gid)) {
        DCHECK_EQ(buf_pools_.at(gid)->buffer_size(), buf_size);
        return;
    }
    buf_pools_[gid] = std::make_unique<utils::BufferPool>(
        fmt::format("IOUring[{}]-{}", uring_id_, gid), buf_size);
}

bool IOUring::RegisterFd(int fd) {
    if (fd_indices_.contains(fd)) {
        HLOG_F(ERROR, "fd {} already registered", fd);
        return false;
    }
    FileDescriptorType fd_type = FileDescriptorUtils::InferType(fd);
    if (fd_type == FileDescriptorType::kInvalid) {
        LOG(FATAL) << "Failed to infer type for fd " << fd;
    }
    if (free_fd_slots_.empty()) {
        LOG(FATAL) << "No more fd slot, consider setting larger --io_uring_fd_slots";
    }
    size_t index = free_fd_slots_.back();
    free_fd_slots_.pop_back();
    int ret = io_uring_register_files_update(
        &ring_, gsl::narrow_cast<uint32_t>(index), &fd, 1);
    if (ret < 0) {
        LOG(FATAL) << "io_uring_register_files_update failed: " << ERRNO_LOGSTR(-ret);
    }
    fd_indices_[fd] = index;
    HLOG_F(INFO, "register fd {} (type {}), {} registered fds in total",
           fd, FileDescriptorUtils::TypeString(fd_type),
           fd_indices_.size());
    Descriptor* desc = &fds_[index];
    desc->fd = fd;
    desc->fd_type = fd_type;
    desc->index = index;
    desc->op_count = 0;
    desc->active_read_op = nullptr;
    desc->last_send_op = nullptr;
    desc->close_op = nullptr;
    return true;
}

#define GET_AND_CHECK_DESC(FD_VAR, DESC_VAR)           \
    if (!fd_indices_.contains(FD_VAR)) {               \
        LOG_F(ERROR, "fd {} not registered", FD_VAR);  \
        return false;                                  \
    }                                                  \
    Descriptor* DESC_VAR = &fds_[fd_indices_[FD_VAR]]; \
    DCHECK_EQ(DESC_VAR->fd, fd);                       \
    if (DESC_VAR->close_op != nullptr) {               \
        LOG_F(WARNING, "fd {} is closing", FD_VAR);    \
        return false;                                  \
    }

bool IOUring::Connect(int fd, const struct sockaddr* addr, size_t addrlen, ConnectCallback cb) {
    GET_AND_CHECK_DESC(fd, desc);
    DCHECK(FileDescriptorUtils::IsSocketType(desc->fd_type))
        << "Connect only applies to socket fds";
    Op* op = AllocConnectOp(desc, addr, addrlen);
    connect_cbs_[op->id] = cb;
    EnqueueOp(op);
    return true;
}

bool IOUring::StartReadInternal(int fd, uint16_t buf_gid, uint16_t flags, ReadCallback cb) {
    GET_AND_CHECK_DESC(fd, desc);
    if ((flags & kOpFlagUseRecv) != 0) {
        DCHECK(FileDescriptorUtils::IsSocketType(desc->fd_type))
            << "Recv only applies to socket fds";
    }
    if (desc->active_read_op != nullptr) {
        HLOG_F(ERROR, "fd {} already registered read callback", fd);
        return false;
    }
    if (!buf_pools_.contains(buf_gid)) {
        HLOG_F(ERROR, "Invalid buf_gid {}", buf_gid);
        return false;
    }
    std::span<char> buf;
    buf_pools_[buf_gid]->Get(&buf);
    Op* op = AllocReadOp(desc, buf_gid, buf, flags);
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
    GET_AND_CHECK_DESC(fd, desc);
    if (desc->active_read_op == nullptr) {
        HLOG_F(WARNING, "fd {} has no registered read callback", fd);
        return false;
    }
    Op* read_op = desc->active_read_op;
    read_op->flags |= kOpFlagCancelled;
    Op* op = AllocCancelOp(read_op->id);
    EnqueueOp(op);
    desc->active_read_op = nullptr;
    return true;
}

bool IOUring::Write(int fd, std::span<const char> data, WriteCallback cb) {
    if (data.size() == 0) {
        return false;
    }
    GET_AND_CHECK_DESC(fd, desc);
    Op* op = AllocWriteOp(desc, data);
    write_cbs_[op->id] = cb;
    EnqueueOp(op);
    return true;
}

bool IOUring::SendAll(int fd, std::span<const char> data, SendAllCallback cb) {
    if (data.size() == 0) {
        return false;
    }
    GET_AND_CHECK_DESC(fd, desc);
    DCHECK(FileDescriptorUtils::IsSocketType(desc->fd_type))
        << "Send only applies to socket fds";
    Op* op = AllocSendAllOp(desc, data);
    sendall_cbs_[op->id] = cb;
    if (desc->last_send_op != nullptr) {
        Op* last_op = desc->last_send_op;
        DCHECK_EQ(op_type(last_op), kSendAll);
        DCHECK_EQ(last_op->next_op, kInvalidOpId);
        last_op->next_op = op->id;
    } else {
        EnqueueOp(op);
    }
    desc->last_send_op = op;
    return true;
}

bool IOUring::SendAll(int fd, const std::vector<std::span<const char>>& data_vec,
                      SendAllCallback cb) {
    GET_AND_CHECK_DESC(fd, desc);
    DCHECK(FileDescriptorUtils::IsSocketType(desc->fd_type))
        << "Send only applies to socket fds";
    Op* first_op = nullptr;
    Op* last_op = desc->last_send_op;
    for (std::span<const char> data : data_vec) {
        if (data.size() > 0) {
            Op* op = AllocSendAllOp(desc, data);
            if (first_op == nullptr) {
                first_op = op;
            }
            if (last_op != nullptr) {
                DCHECK_EQ(op_type(last_op), kSendAll);
                DCHECK_EQ(last_op->next_op, kInvalidOpId);
                last_op->next_op = op->id;
            }
            last_op = op;
        }
    }
    if (first_op == nullptr) {
        return false;
    }
    DCHECK(last_op != desc->last_send_op);
    Op* op = first_op;
    while (op != last_op) {
        op->root_op = last_op->id;
        DCHECK_NE(op->next_op, kInvalidOpId);
        DCHECK(ops_.contains(op->next_op));
        op = ops_[op->next_op];
    }
    sendall_cbs_[first_op->id] = cb;
    if (desc->last_send_op == nullptr) {
        EnqueueOp(first_op);
    }
    desc->last_send_op = last_op;
    return true;
}

bool IOUring::Close(int fd, CloseCallback cb) {
    GET_AND_CHECK_DESC(fd, desc);
    if (desc->active_read_op != nullptr) {
        StopReadOrRecv(fd);
    }
    Op* op = AllocCloseOp(fd);
    desc->close_op = op;
    close_cbs_[op->id] = cb;
    if (desc->op_count == 0) {
        UnregisterFd(desc);
        EnqueueOp(op);
    }
    return true;
}

#undef GET_AND_CHECK_DESC

void IOUring::EventLoopRunOnce(size_t* inflight_ops) {
    struct io_uring_cqe* cqe = nullptr;
    uint32_t nr_wait = absl::GetFlag(FLAGS_io_uring_cq_nr_wait);
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
    if (VLOG_IS_ON(2)) {
        VLOG(2) << "Inflight ops:";
        for (const auto& [op_id, op] : ops_) {
            DCHECK_EQ(op->id, op_id);
            VLOG_F(2, "id={}, type={}, fd={}",
                   (op->id >> 8), kOpTypeStr[op_type(op)], op_fd(op));
        }
    }
    *inflight_ops = ops_.size();
}

#define ALLOC_OP(TYPE, OP_VAR)        \
    Op* OP_VAR = op_pool_.Get();      \
    uint64_t id = next_op_id_++;      \
    OP_VAR->id = (id << 8) + TYPE;    \
    OP_VAR->fd = -1;                  \
    OP_VAR->desc = nullptr;           \
    OP_VAR->buf_gid = 0;              \
    OP_VAR->flags = 0;                \
    OP_VAR->buf = nullptr;            \
    OP_VAR->buf_len = 0;              \
    OP_VAR->root_op = kInvalidOpId;   \
    OP_VAR->next_op = kInvalidOpId;   \
    ops_[op->id] = op

IOUring::Op* IOUring::AllocConnectOp(Descriptor* desc,
                                     const struct sockaddr* addr, size_t addrlen) {
    ALLOC_OP(kConnect, op);
    op->desc = desc;
    op->addr = addr;
    op->addrlen = gsl::narrow_cast<uint32_t>(addrlen);
    desc->op_count++;
    return op;
}

IOUring::Op* IOUring::AllocReadOp(Descriptor* desc, uint16_t buf_gid, std::span<char> buf,
                                  uint16_t flags) {
    ALLOC_OP(kRead, op);
    op->desc = desc;
    op->buf_gid = buf_gid;
    op->flags = flags;
    op->buf = buf.data();
    DCHECK_EQ(bits::HighHalf64(buf.size()), 0U);
    op->buf_len = bits::LowHalf64(buf.size());
    desc->op_count++;
    return op;
}

IOUring::Op* IOUring::AllocWriteOp(Descriptor* desc, std::span<const char> data) {
    ALLOC_OP(kWrite, op);
    op->desc = desc;
    op->data = data.data();
    DCHECK_EQ(bits::HighHalf64(data.size()), 0U);
    op->data_len = bits::LowHalf64(data.size());
    desc->op_count++;
    return op;
}

IOUring::Op* IOUring::AllocSendAllOp(Descriptor* desc, std::span<const char> data) {
    ALLOC_OP(kSendAll, op);
    op->desc = desc;
    op->data = data.data();
    DCHECK_EQ(bits::HighHalf64(data.size()), 0U);
    op->data_len = bits::LowHalf64(data.size());
    desc->op_count++;
    return op;
}

IOUring::Op* IOUring::AllocCloseOp(int fd) {
    ALLOC_OP(kClose, op);
    op->fd = fd;
    return op;
}

IOUring::Op* IOUring::AllocCancelOp(uint64_t op_id) {
    ALLOC_OP(kCancel, op);
    op->root_op = op_id;
    return op;
}

#undef ALLOC_OP

void IOUring::UnregisterFd(Descriptor* desc) {
    int fd = desc->fd;
    size_t index = desc->index;
    DCHECK(fd_indices_.contains(fd));
    DCHECK_EQ(fd_indices_[fd], index);
    DCHECK_EQ(&fds_[index], desc);
    int value = -1;
    int ret = io_uring_register_files_update(
        &ring_, gsl::narrow_cast<uint32_t>(index), &value, 1);
    if (ret < 0) {
        LOG(FATAL) << "io_uring_register_files_update failed: " << ERRNO_LOGSTR(-ret);
    }
    fds_[index].fd = -1;
    free_fd_slots_.push_back(index);
    fd_indices_.erase(fd);
    HLOG_F(INFO, "unregister fd {}, {} registered fds in total", fd, fd_indices_.size());
}

void IOUring::EnqueueOp(Op* op) {
    VLOG_F(2, "EnqueueOp: id={}, type={}, fd={}",
           (op->id >> 8), kOpTypeStr[op_type(op)], op_fd(op));
    struct io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
    unsigned flags = 0;
    switch (op_type(op)) {
    case kConnect:
        io_uring_prep_connect(sqe, op_fd_idx(op), op->addr, op->addrlen);
        flags = IOSQE_FIXED_FILE;
        break;
    case kRead:
        DCHECK_NOTNULL(op->desc)->active_read_op = op;
        flags = IOSQE_FIXED_FILE;
        if (op->flags & kOpFlagUseRecv) {
            io_uring_prep_recv(sqe, op_fd_idx(op), op->buf, op->buf_len, 0);
        } else {
            io_uring_prep_read(sqe, op_fd_idx(op), op->buf, op->buf_len, 0);
            // A weird hack
            if (op->desc->fd_type == FileDescriptorType::kFifo) {
                flags |= IOSQE_ASYNC;
            }
        }
        break;
    case kWrite:
        io_uring_prep_write(sqe, op_fd_idx(op), op->data, op->data_len, 0);
        flags = IOSQE_FIXED_FILE;
        break;
    case kSendAll:
        io_uring_prep_send(sqe, op_fd_idx(op), op->data, op->data_len, 0);
        flags = IOSQE_FIXED_FILE;
        break;
    case kClose:
        io_uring_prep_close(sqe, op->fd);
        break;
    case kCancel:
        VLOG_F(1, "Going to cancel op {} (type {}): ",
               (op->root_op >> 8), kOpTypeStr[op->root_op & 0xff]);
        io_uring_prep_cancel(sqe, reinterpret_cast<void*>(op->root_op), 0);
        break;
    default:
        UNREACHABLE();
    }
    if (flags != 0) {
        io_uring_sqe_set_flags(sqe, flags);
    }
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(op->id));
}

void IOUring::OnOpComplete(Op* op, struct io_uring_cqe* cqe) {
    int res = cqe->res;
    VLOG(2) << fmt::format("Op completed: id={}, type={}, fd={}, res={}",
                           (op->id >> 8), kOpTypeStr[op_type(op)], op_fd(op), res);
    Op* next_op = nullptr;
    switch (op_type(op)) {
    case kConnect:
        HandleConnectComplete(op, res);
        break;
    case kRead:
        HandleReadOpComplete(op, res, &next_op);
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
            LOG_F(WARNING, "Failed to cancel op {} (type {}): {}",
                  (op->root_op >> 8), kOpTypeStr[op->root_op & 0xff],
                  ERRNO_LOGSTR(-res));
        }
        break;
    default:
        UNREACHABLE();
    }
    if (next_op == nullptr && op->next_op != kInvalidOpId) {
        DCHECK(ops_.contains(op->next_op));
        next_op = ops_[op->next_op];
    }
    if (next_op != nullptr) {
        EnqueueOp(next_op);
    }
    if (op->desc != nullptr) {
        op->desc->op_count--;
        if (op->desc->op_count == 0 && op->desc->close_op != nullptr) {
            UnregisterFd(op->desc);
            EnqueueOp(op->desc->close_op);
        }
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

void IOUring::HandleReadOpComplete(Op* op, int res, Op** next_op) {
    DCHECK_EQ(op_type(op), kRead);
    DCHECK(read_cbs_.contains(op->id));
    DCHECK_NOTNULL(op->desc)->active_read_op = nullptr;
    bool repeat = false;
    if (res >= 0) {
        std::span<const char> data(op->buf, static_cast<size_t>(res));
        repeat = read_cbs_[op->id](0, data);
    } else if (res == -EAGAIN || res == -EINTR) {
        repeat = true;
    } else if (res == -ECANCELED) {
        LOG(INFO) << "ReadOp cancelled";
    } else {
        errno = -res;
        repeat = read_cbs_[op->id](-1, EMPTY_CHAR_SPAN);
    }
    if ((op->flags & kOpFlagRepeat) != 0
            && (op->flags & kOpFlagCancelled) == 0
            && op->desc->close_op == nullptr
            && repeat) {
        Op* new_op = AllocReadOp(op->desc, op->buf_gid,
                                 std::span<char>(op->buf, op->buf_len), op->flags);
        read_cbs_[new_op->id].swap(read_cbs_[op->id]);
        read_cbs_.erase(op->id);
        *next_op = new_op;
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
        write_cbs_[op->id](0, gsl::narrow_cast<size_t>(res));
    } else {
        errno = -res;
        write_cbs_[op->id](-1, 0);
    }
    write_cbs_.erase(op->id);
}

void IOUring::HandleSendallOpComplete(Op* op, int res, Op** next_op) {
    DCHECK_EQ(op_type(op), kSendAll);
    DCHECK(sendall_cbs_.contains(op->id));
    DCHECK(op->desc != nullptr);
    SendAllCallback cb;
    cb.swap(sendall_cbs_[op->id]);
    sendall_cbs_.erase(op->id);
    if (res >= 0) {
        size_t nwrite = gsl::narrow_cast<size_t>(res);
        if (nwrite == op->data_len) {
            if (op->root_op == kInvalidOpId) {
                cb(0);
                if (op->desc->last_send_op == op) {
                    DCHECK_EQ(op->next_op, kInvalidOpId);
                    op->desc->last_send_op = nullptr;
                }
            } else {
                DCHECK_NE(op->next_op, kInvalidOpId);
                sendall_cbs_[op->next_op].swap(cb);
            }
        } else {
            std::span<const char> remaining_data(op->data + nwrite, op->data_len - nwrite);
            Op* new_op = AllocSendAllOp(op->desc, remaining_data);
            new_op->root_op = op->root_op;
            new_op->next_op = op->next_op;
            sendall_cbs_[new_op->id].swap(cb);
            if (op->desc->last_send_op == op) {
                DCHECK_EQ(op->next_op, kInvalidOpId);
                op->desc->last_send_op = new_op;
            }
            *next_op = new_op;
        }
    } else {
        errno = -res;
        cb(-1);
        if (op->root_op != kInvalidOpId) {
            uint64_t final = op->root_op;
            DCHECK(ops_.contains(final));
            Op* final_op = ops_[final];
            DCHECK_EQ(final_op->root_op, kInvalidOpId);
            uint64_t cur = op->next_op;
            while (cur != final) {
                DCHECK_NE(cur, kInvalidOpId);
                DCHECK(ops_.contains(cur));
                Op* tmp_op = ops_[cur];
                DCHECK_EQ(tmp_op->desc, op->desc);
                op->desc->op_count--;
                op_pool_.Return(tmp_op);
                cur = tmp_op->next_op;
            }
            op->next_op = final_op->next_op;
            if (op->desc->last_send_op == final_op) {
                DCHECK_EQ(final_op->next_op, kInvalidOpId);
                op->desc->last_send_op = nullptr;
            }
            op->desc->op_count--;
            op_pool_.Return(final_op);
        } else {
            if (op->desc->last_send_op == op) {
                DCHECK_EQ(op->next_op, kInvalidOpId);
                op->desc->last_send_op = nullptr;
            }
        }
    }
}

void IOUring::HandleCloseOpComplete(Op* op, int res) {
    DCHECK_EQ(op_type(op), kClose);
    if (res < 0) {
        LOG_F(FATAL, "Failed to close fd {}: {}", op->fd, ERRNO_LOGSTR(-res));
    }
    DCHECK(close_cbs_.contains(op->id));
    close_cbs_[op->id]();
    close_cbs_.erase(op->id);
}

}  // namespace server
}  // namespace faas
