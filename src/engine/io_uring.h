#pragma once

#include "base/common.h"
#include "common/stat.h"
#include "utils/object_pool.h"
#include "utils/buffer_pool.h"

#include <liburing.h>

namespace faas {
namespace engine {

class IOUring {
public:
    explicit IOUring();
    ~IOUring();

    void PrepareBuffers(uint16_t gid, size_t buf_size);
    bool RegisterFd(int fd);
    bool UnregisterFd(int fd);

    typedef std::function<bool(int /* status */, std::span<const char> /* data */)> ReadCallback;
    bool StartRead(int fd, uint16_t buf_gid, ReadCallback cb);
    bool StartRecv(int fd, uint16_t buf_gid, ReadCallback cb);
    bool StopReadOrRecv(int fd);

    // Partial write may happen. The caller is responsible for handling partial writes.
    typedef std::function<void(int /* status */, size_t /* nwrite */)> WriteCallback;
    bool Write(int fd, std::span<const char> data, WriteCallback cb);

    // Only works for sockets. Partial write will not happen.
    // IOUring implementation will correctly order all SendAll writes.
    typedef std::function<void(int /* status */)> SendAllCallback;
    bool SendAll(int sockfd, std::span<const char> data, SendAllCallback cb);

    typedef std::function<void()> CloseCallback;
    bool Close(int fd, CloseCallback cb);

    void EventLoopRunOnce(int* inflight_ops);

private:
    int uring_id_;
    static std::atomic<int> next_uring_id_;
    struct io_uring ring_;
    struct __kernel_timespec cqe_wait_timeout;

    std::vector<int> fds_;
    absl::flat_hash_map</* fd */ int, /* index */ size_t> fd_indices_;

    absl::flat_hash_map</* gid */ uint16_t, std::unique_ptr<utils::BufferPool>> buf_pools_;
    std::vector<int> ref_counts_;

    enum OpType { kRead, kWrite, kSendAll, kClose, kCancel };
    enum {
        kOpFlagRepeat    = 1 << 0,
        kOpFlagUseRecv   = 1 << 1,
        kOpFlagCancelled = 1 << 2,
    };
    static constexpr uint64_t kInvalidOpId = std::numeric_limits<uint64_t>::max();
    static constexpr size_t kInvalidFdIndex = std::numeric_limits<size_t>::max();
    struct Op {
        uint64_t id;         // Lower 8-bit stores type
        size_t   fd_idx;     // Used by kRead, kWrite, kSendAll, kClose
        uint16_t buf_gid;    // Used by kRead
        uint16_t flags;
        char*    buf;        // Used by kRead, kWrite, kSendAll
        size_t   buf_len;    // Used by kRead, kWrite, kSendAll
        uint64_t next_op;    // Used by kSendAll
    };

    uint64_t next_op_id_;
    utils::SimpleObjectPool<Op> op_pool_;
    absl::flat_hash_map</* op_id */ uint64_t, Op*> ops_;

    std::vector<Op*> read_ops_;
    std::vector<Op*> last_send_op_;

    absl::flat_hash_map</* op_id */ uint64_t, ReadCallback> read_cbs_;
    absl::flat_hash_map</* op_id */ uint64_t, WriteCallback> write_cbs_;
    absl::flat_hash_map</* op_id */ uint64_t, SendAllCallback> sendall_cbs_;
    std::vector<CloseCallback> close_cbs_;

    stat::Counter ev_loop_counter_;
    stat::Counter wait_timeout_counter_;
    stat::Counter completed_ops_counter_;
    stat::StatisticsCollector<int> io_uring_enter_time_stat_;
    stat::StatisticsCollector<int> completed_ops_stat_;
    stat::StatisticsCollector<int> ev_loop_time_stat_;
    stat::StatisticsCollector<int> average_op_time_stat_;

    inline OpType op_type(const Op* op) { return gsl::narrow_cast<OpType>(op->id & 0xff); }

    bool StartReadInternal(int fd, uint16_t buf_gid, uint16_t flags, ReadCallback cb);

    Op* AllocReadOp(size_t fd_idx, uint16_t buf_gid, std::span<char> buf, uint16_t flags);
    Op* AllocWriteOp(size_t fd_idx, std::span<const char> data);
    Op* AllocSendAllOp(size_t fd_idx, std::span<const char> data);
    Op* AllocCloseOp(size_t fd_idx);
    Op* AllocCancelOp(uint64_t op_id);

    void EnqueueOp(Op* op);
    void OnOpComplete(Op* op, struct io_uring_cqe* cqe);
    void RefFd(size_t fd_idx);
    void UnrefFd(size_t fd_idx);

    void HandleReadOpComplete(Op* op, int res);
    void HandleWriteOpComplete(Op* op, int res);
    void HandleSendallOpComplete(Op* op, int res);

    DISALLOW_COPY_AND_ASSIGN(IOUring);
};

#define URING_CHECK_OK(URING_CALL)                     \
    do {                                               \
        bool ret = URING_CALL;                         \
        LOG_IF(FATAL, !ret) << "IOUring call failed";  \
    } while (0)

#define URING_DCHECK_OK(URING_CALL)                    \
    do {                                               \
        bool ret = URING_CALL;                         \
        DLOG_IF(FATAL, !ret) << "IOUring call failed"; \
    } while (0)

}  // namespace engine
}  // namespace faas
