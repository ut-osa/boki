#pragma once

#include "base/common.h"
#include "common/stat.h"
#include "utils/object_pool.h"
#include "utils/buffer_pool.h"

#include <liburing.h>

namespace faas {
namespace server {

class IOUring {
public:
    explicit IOUring();
    ~IOUring();

    void PrepareBuffers(uint16_t gid, size_t buf_size);
    bool RegisterFd(int fd);

    typedef std::function<void(int /* status */)> ConnectCallback;
    bool Connect(int fd, const struct sockaddr* addr, size_t addrlen, ConnectCallback cb);

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
    bool SendAll(int sockfd, const std::vector<std::span<const char>>& data_vec,
                 SendAllCallback cb);

    typedef std::function<void()> CloseCallback;
    bool Close(int fd, CloseCallback cb);

    void EventLoopRunOnce(int* inflight_ops);

private:
    int uring_id_;
    static std::atomic<int> next_uring_id_;
    struct io_uring ring_;
    struct __kernel_timespec cqe_wait_timeout_;

    std::string log_header_;

    absl::flat_hash_map</* gid */ uint16_t, std::unique_ptr<utils::BufferPool>> buf_pools_;

    struct Op;
    struct Descriptor {
        int fd;
        size_t index;
        size_t op_count;
        Op* active_read_op;
        Op* last_send_op;
        Op* close_op;
    };
    std::vector<Descriptor> fds_;
    std::vector<size_t> free_fd_slots_;
    absl::flat_hash_map</* fd */ int, /* index */ size_t> fd_indices_;

    enum OpType {
        kConnect = 0,
        kRead    = 1,
        kWrite   = 2,
        kSendAll = 3,
        kClose   = 4,
        kCancel  = 5
    };
    static constexpr const char* kOpTypeStr[] = {
        "Connect",
        "Read",
        "Write",
        "SendAll",
        "Close",
        "Cancel"
    };

    enum {
        kOpFlagRepeat    = 1 << 0,
        kOpFlagUseRecv   = 1 << 1,
        kOpFlagCancelled = 1 << 2,
    };
    static constexpr uint64_t kInvalidOpId = std::numeric_limits<uint64_t>::max();
    static constexpr size_t kInvalidFdIndex = std::numeric_limits<size_t>::max();
    struct Op {
        uint64_t id;         // Lower 8-bit stores type
        int fd;              // Used by kClose
        Descriptor* desc;    // Used by kConnect, kRead, kWrite, kSendAll
        uint16_t buf_gid;    // Used by kRead
        uint16_t flags;
        union {
            char* buf;                    // Used by kRead
            const char* data;             // Used by kWrite, kSendAll
            const struct sockaddr* addr;  // Used by kConnect
        };
        union {
            size_t buf_len;   // Used by kRead
            size_t data_len;  // Used by kWrite, kSendAll
            size_t addrlen;   // Used by kConnect
        };
        uint64_t root_op;    // Used by kSendAll
        uint64_t next_op;    // Used by kSendAll, kCancel
    };

    uint64_t next_op_id_;
    utils::SimpleObjectPool<Op> op_pool_;
    absl::flat_hash_map</* op_id */ uint64_t, Op*> ops_;

    absl::flat_hash_map</* op_id */ uint64_t, ConnectCallback> connect_cbs_;
    absl::flat_hash_map</* op_id */ uint64_t, ReadCallback> read_cbs_;
    absl::flat_hash_map</* op_id */ uint64_t, WriteCallback> write_cbs_;
    absl::flat_hash_map</* op_id */ uint64_t, SendAllCallback> sendall_cbs_;
    absl::flat_hash_map</* op_id */ uint64_t, CloseCallback> close_cbs_;

    stat::Counter ev_loop_counter_;
    stat::Counter wait_timeout_counter_;
    stat::Counter completed_ops_counter_;
    stat::StatisticsCollector<int> io_uring_enter_time_stat_;
    stat::StatisticsCollector<int> completed_ops_stat_;
    stat::StatisticsCollector<int> ev_loop_time_stat_;
    stat::StatisticsCollector<int> average_op_time_stat_;

    inline OpType op_type(const Op* op) { return gsl::narrow_cast<OpType>(op->id & 0xff); }
    inline int op_fd(const Op* op) {
        if (op->fd != -1) {
            return op->fd;
        } else if (op->desc != nullptr) {
            return op->desc->fd;
        }
        return -1;
    }
    inline size_t op_fd_idx(const Op* op) {
        return DCHECK_NOTNULL(op->desc)->index;
    }

    bool StartReadInternal(int fd, uint16_t buf_gid, uint16_t flags, ReadCallback cb);

    Op* AllocConnectOp(Descriptor* desc, const struct sockaddr* addr, size_t addrlen);
    Op* AllocReadOp(Descriptor* desc, uint16_t buf_gid, std::span<char> buf, uint16_t flags);
    Op* AllocWriteOp(Descriptor* desc, std::span<const char> data);
    Op* AllocSendAllOp(Descriptor* desc, std::span<const char> data);
    Op* AllocCloseOp(int fd);
    Op* AllocCancelOp(uint64_t op_id);

    void UnregisterFd(Descriptor* desc);
    void EnqueueOp(Op* op);
    void OnOpComplete(Op* op, struct io_uring_cqe* cqe);

    void HandleConnectComplete(Op* op, int res);
    void HandleReadOpComplete(Op* op, int res, Op** next_op);
    void HandleWriteOpComplete(Op* op, int res);
    void HandleSendallOpComplete(Op* op, int res, Op** next_op);
    void HandleCloseOpComplete(Op* op, int res);

    DISALLOW_COPY_AND_ASSIGN(IOUring);
};

}  // namespace server
}  // namespace faas

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
