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
    std::string_view              value;
    std::vector<std::string_view> values;
    std::span<const char>         data;
    const struct Stat*            stat;
};

class ZKSession {
public:
    explicit ZKSession(std::string_view host);
    ~ZKSession();

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    typedef std::function<void(int /* type */, std::string_view /* path */)> WatcherFn;
    typedef std::function<void(int /* status */, const ZKResult&,
                               bool* /* remove_watch */)> Callback;

    void Create(std::string_view path, std::span<const char> value, int mode, Callback cb);
    void Delete(std::string_view path, int version, Callback cb);
    void Exists(std::string_view path, WatcherFn watcher_fn, Callback cb);
    void Get(std::string_view path, WatcherFn watcher_fn, Callback cb);
    void Set(std::string_view path, std::span<const char> value, int version, Callback cb);
    void GetChildren(std::string_view path, WatcherFn watcher_fn, Callback cb);

    bool GetOrWait(std::string_view path, std::string* value);

private:
    enum State { kCreated, kRunning, kStopped };
    std::atomic<State> state_;
    std::string host_;
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
