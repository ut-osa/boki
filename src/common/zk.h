#pragma once

#ifndef __FAAS_SRC
#error common/zk.h cannot be included outside
#endif

#include "base/common.h"
#include "base/thread.h"
#include "utils/object_pool.h"
#include "utils/appendable_buffer.h"

#include <zookeeper/zookeeper.h>

namespace faas {
namespace zk {

struct ZKResult {
    std::string_view              path;
    std::vector<std::string_view> paths;
    std::span<const char>         data;
    const struct Stat*            stat;
};

struct ZKStatus {
public:
    static constexpr int kUndefinedValue = -65536;
    explicit ZKStatus(int code = kUndefinedValue) : code(code) {}

    bool ok() const { return code == ZOK; }
    bool undefined() const { return code == kUndefinedValue; }
    bool IsBadArguments() const { return code == ZBADARGUMENTS; }
    bool IsNoNode() const { return code == ZNONODE; }
    bool IsNodeExist() const { return code == ZNODEEXISTS; }
    bool IsNotEmpty() const { return code == ZNOTEMPTY; }
    bool IsBadVersion() const { return code == ZBADVERSION; }
    bool IsNoChildrenForEphemerals() const { return code == ZNOCHILDRENFOREPHEMERALS; }
    std::string_view ToString() const { return std::string_view(zerror(code)); }

    int code;
};

struct ZKEvent {
public:
    explicit ZKEvent(int type) : type(type) {}

    bool IsCreated() const { return type == ZOO_CREATED_EVENT; }
    bool IsDeleted() const { return type == ZOO_DELETED_EVENT; }
    bool IsChanged() const { return type == ZOO_CHANGED_EVENT; }
    bool IsChild() const { return type == ZOO_CHILD_EVENT; }

    int type;
};

enum class ZKCreateMode : int {
    // Copied from zookeeper-client-c/src/zookeeper.c
    kPersistent           = 0,
    kEphemeral            = 1,
    kPersistentSequential = 2,
    kEphemeralSequential  = 3,
    kContainer            = 4
};

bool ZKParseSequenceNumber(std::string_view created_path, uint64_t* parsed);

class ZKSession {
public:
    ZKSession(std::string_view host, std::string_view root_path);
    ~ZKSession();

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    typedef std::function<void(ZKEvent /* event */, std::string_view /* path */)> WatcherFn;
    typedef std::function<void(ZKStatus /* status */, const ZKResult& /* result */,
                               bool* /* remove_watch */)> Callback;

    // If succeeded, `result.path` is set to the path of newly created node
    void Create(std::string_view path, std::span<const char> value,
                ZKCreateMode mode, Callback cb);
    // Nothing is set in result
    void Delete(std::string_view path, Callback cb);
    void DeleteWithVersion(std::string_view path, int version, Callback cb);
    // If node exists, `result.stat` is set
    void Exists(std::string_view path, WatcherFn watcher_fn, Callback cb);
    // If node exists, both `result.data` and `result.stat` are set
    void Get(std::string_view path, WatcherFn watcher_fn, Callback cb);
    // If succeeded, `result.stat` is set
    void Set(std::string_view path, std::span<const char> value, Callback cb);
    void SetWithVersion(std::string_view path, std::span<const char> value,
                        int version, Callback cb);
    // If succeeded, `result.paths` is set to paths of children
    void GetChildren(std::string_view path, WatcherFn watcher_fn, Callback cb);

    // Synchronous helper APIs
    // They cannot be called from callbacks of asynchronous APIs above
    //
    // Synchronously create a new node
    ZKStatus CreateSync(std::string_view path, std::span<const char> value,
                        ZKCreateMode mode, std::string* created_path);
    // Synchronously delete a node.
    ZKStatus DeleteSync(std::string_view path);
    // Get the value of `path`. If `path` not exists, will wait for its creation
    ZKStatus GetOrWaitSync(std::string_view path, std::string* value);

private:
    enum State { kCreated, kRunning, kStopped };
    std::atomic<State> state_;
    std::string host_;
    std::string root_path_;
    zhandle_t* handle_;

    base::Thread event_loop_thread_;
    int stop_eventfd_;
    int new_op_eventfd_;

    struct Op;
    struct Watch {
        ZKSession*  sess;
        Op*         op;
        std::string path;
        WatcherFn   cb;
        bool        removed;
        bool        triggered;
    };

    enum OpType { kCreate, kDelete, kExists, kGet, kSet, kGetChildren };
    struct Op {
        ZKSession*              sess;
        OpType                  type;
        std::string             path;
        utils::AppendableBuffer value;
        int                     create_mode;
        int                     data_version;
        Watch*                  watch;
        Callback                cb;
    };

    absl::Mutex mu_;
    utils::SimpleObjectPool<Op>     op_pool_     ABSL_GUARDED_BY(mu_);
    std::vector<Op*>                pending_ops_ ABSL_GUARDED_BY(mu_);
    utils::SimpleObjectPool<Watch>  watch_pool_  ABSL_GUARDED_BY(mu_);

    std::vector<Op*>    completed_ops_;
    std::vector<Watch*> completed_watches_;

    void EnqueueNewOp(OpType type, std::string_view path,
                      WatcherFn watcher_fn, Callback cb,
                      std::function<void(Op*)> setup_fn);
    void ProcessPendingOps();
    void DoOp(Op* op);
    void OpCompleted(Op* op, int rc, const ZKResult& result);
    void OnWatchTriggered(Watch* watch, int type, int state, std::string_view path);
    void ReclaimResource();

    void EventLoopThreadMain();
    bool WithinMyEventLoopThread();

    static ZKResult EmptyResult();
    static ZKResult StringResult(const char* string);
    static ZKResult StringsResult(const struct String_vector* strings);
    static ZKResult DataResult(const char* data, int data_len, const struct Stat* stat);
    static ZKResult StatResult(const struct Stat* stat);

    static void WatcherCallback(zhandle_t* handle, int type, int state,
                                const char* path, void* watcher_ctx);
    static void VoidCompletionCallback(int rc, const void* data);
    static void StringCompletionCallback(int rc, const char* value, const void* data);
    static void StringsCompletionCallback(int rc, const struct String_vector* strings,
                                          const void* data);
    static void DataCompletionCallback(int rc, const char* value, int value_len,
                                       const struct Stat* stat, const void* data);
    static void StatCompletionCallback(int rc, const struct Stat* stat, const void* data);

    DISALLOW_COPY_AND_ASSIGN(ZKSession);
};

}  // namespace zk
}  // namespace faas
