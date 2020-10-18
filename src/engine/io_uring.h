#pragma once

#include "base/common.h"
#include "utils/object_pool.h"
#include "utils/buffer_pool.h"

#include <liburing.h>

namespace faas {
namespace engine {

class IOUring {
public:
    explicit IOUring(int entries);
    ~IOUring();

    void PrepareBuffers(uint16_t gid, size_t buf_size);

    typedef std::function<void(int /* fd */, int /* status */,
                               std::span<const char> /* data */)> ReadCallback;
    bool StartRead(int fd, uint16_t buf_gid, bool repeat, ReadCallback cb);
    bool StopRead(int fd);

    // Partial write may happen. The caller is responsible for handling partial writes.
    typedef std::function<void(int /* fd */, int /* status */, size_t /* nwrite */)> WriteCallback;
    bool Write(int fd, std::span<const char> data, WriteCallback cb);

    // Only works for sockets. Partial write will not happen.
    // IOUring implementation will correctly order all SendAll writes.
    typedef std::function<void(int /* fd */, int /* status */)> SendAllCallback;
    bool SendAll(int sockfd, std::span<const char> data, SendAllCallback cb);

    typedef std::function<void(int /* fd */)> CloseCallback;
    bool Close(int fd, CloseCallback cb);

    void EventLoopRunOnce(int* inflight_ops);

private:
    struct io_uring ring_;

    absl::flat_hash_map</* gid */ uint16_t, std::unique_ptr<utils::BufferPool>> buf_pools_;
    absl::flat_hash_map</* fd */ int, int> ref_counts_;
    absl::flat_hash_map</* fd */ int, ReadCallback> read_cbs_;

    enum OpType { kRead, kWrite, kSendAll, kClose, kCancel };
    enum {
        kOpFlagRepeat = 1 << 0,
    };
    static constexpr uint64_t kInvalidOpId = ~0ULL;
    static constexpr int kInvalidFd = -1;
    struct Op {
        uint64_t id;         // Lower 8-bit stores type
        int      fd;         // Used by kRead, kWrite, kSendAll, kClose
        uint16_t buf_gid;    // Used by kRead
        uint16_t flags;
        char*    buf;        // Used by kRead, kSendAll
        size_t   buf_len;    // Used by kRead, kSendAll
        uint64_t next_op;    // Used by kSendAll
    } __attribute__ ((packed));
    static_assert(sizeof(Op) == 40, "Unexpected Op size");

    uint64_t next_op_id_;
    utils::SimpleObjectPool<Op> op_pool_;
    absl::flat_hash_map</* op_id */ uint64_t, Op*> ops_;
    absl::flat_hash_map</* fd */ int, Op*> read_ops_;
    absl::flat_hash_map</* op_id */ uint64_t, WriteCallback> write_cbs_;
    absl::flat_hash_map</* op_id */ uint64_t, SendAllCallback> sendall_cbs_;
    absl::flat_hash_map</* fd */ int, Op*> last_send_op_;
    absl::flat_hash_map</* fd */ int, CloseCallback> close_cbs_;

    inline OpType op_type(const Op* op) { return gsl::narrow_cast<OpType>(op->id & 0xff); }

    Op* EnqueueRead(int fd, uint16_t buf_gid, std::span<char> buf, bool repeat);
    Op* EnqueueWrite(int fd, std::span<const char> data);
    Op* EnqueueClose(int fd);
    Op* EnqueueCancel(uint64_t op_id);

    Op* AllocSendAllOp(int fd, std::span<const char> data);
    void EnqueueSendAllOp(Op* op);

    void OnOpComplete(Op* op, struct io_uring_cqe* cqe);
    void RefFd(int fd);
    void UnrefFd(int fd);

    void HandleReadOpComplete(Op* op, int res);
    void HandleWriteOpComplete(Op* op, int res);
    void HandleSendallOpComplete(Op* op, int res);

    DISALLOW_COPY_AND_ASSIGN(IOUring);
};

}  // namespace engine
}  // namespace faas
