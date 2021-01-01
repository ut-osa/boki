#include "common/zk.h"

#include <sys/types.h>
#include <sys/eventfd.h>
#include <sys/time.h>
#include <fcntl.h>
#include <poll.h>

#define log_header_ "ZKSession: "

#include <absl/flags/flag.h>

ABSL_FLAG(int, zk_recv_timeout_ms, 2000, "");
ABSL_FLAG(std::string, zk_logfile, "", "");

#define ZK_CHECK_OK(ZK_CALL)                                  \
    do {                                                      \
        int ret = ZK_CALL;                                    \
        LOG_IF(FATAL, ret != 0) << "zookeeper call failed: "  \
                                << zerror(ret);               \
    } while (0)

#define ZK_DCHECK_OK(ZK_CALL)                                 \
    do {                                                      \
        int ret = ZK_CALL;                                    \
        DLOG_IF(FATAL, ret != 0) << "zookeeper call failed: " \
                                 << zerror(ret);              \
    } while (0)

namespace faas {
namespace zk {

ZKSession::ZKSession(std::string_view host)
    : state_(kCreated),
      host_(host),
      handle_(nullptr),
      event_loop_thread_("ZK/EL",
                         absl::bind_front(&ZKSession::EventLoopThreadMain, this)) {
    stop_eventfd_ = eventfd(0, 0);
    PCHECK(stop_eventfd_ >= 0) << "Failed to create eventfd";
    new_op_eventfd_ = eventfd(0, 0);
    PCHECK(new_op_eventfd_ >= 0) << "Failed to create eventfd";
}

ZKSession::~ZKSession() {
    if (handle_ != nullptr) {
        ZK_CHECK_OK(zookeeper_close(handle_));
    }
    PCHECK(close(stop_eventfd_) == 0) << "Failed to close eventfd";
    PCHECK(close(new_op_eventfd_) == 0) << "Failed to close eventfd";
}

namespace {
static FILE*           log_stream;
static absl::once_flag log_stream_init;

static void ZKLogCallback(const char *message) {
    absl::call_once(log_stream_init, [] () {
        std::string logfile = absl::GetFlag(FLAGS_zk_logfile);
        if (!logfile.empty()) {
            log_stream = fopen(logfile.c_str(), "a");
            if (log_stream == nullptr) {
                LOG(FATAL) << "Failed to open file: " << logfile;
            }
        } else {
            log_stream = nullptr;
        }
    });
    if (log_stream != nullptr) {
        fprintf(log_stream, "%s\n", message);
        fflush(log_stream);
    }
}
}  // namespace

void ZKSession::Start() {
    DCHECK(state_.load() == kCreated);
    handle_ = zookeeper_init2(
        /* host= */         host_.c_str(),
        /* watcher_fn= */   nullptr,
        /* recv_timeout= */ absl::GetFlag(FLAGS_zk_recv_timeout_ms),
        /* clientid= */     nullptr,
        /* context= */      this,
        /* flags= */        0,
        /* log_callback= */ &ZKLogCallback);
    if (handle_ == nullptr) {
        PLOG(FATAL) << "zookeeper_init failed";
    }
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void ZKSession::ScheduleStop() {
    HLOG(INFO) << "Scheduled to stop";
    DCHECK(stop_eventfd_ >= 0);
    PCHECK(eventfd_write(stop_eventfd_, 1) == 0) << "eventfd_write failed";
}

void ZKSession::WaitForFinish() {
    DCHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

void ZKSession::Create(std::string_view path, std::span<const char> value,
                       int mode, Callback cb) {
    EnqueueNewOp(kCreate, path, nullptr, cb, [value, mode] (Op* op) {
        op->value.ResetWithData(value);
        op->create_mode = mode;
    });
}

void ZKSession::Delete(std::string_view path, int version, Callback cb) {
    EnqueueNewOp(kDelete, path, nullptr, cb, [version] (Op* op) {
        op->data_version = version;
    });
}

void ZKSession::Exists(std::string_view path, WatcherFn wathcer_fn, Callback cb) {
    EnqueueNewOp(kExists, path, wathcer_fn, cb, nullptr);
}

void ZKSession::Get(std::string_view path, WatcherFn wathcer_fn, Callback cb) {
    EnqueueNewOp(kGet, path, wathcer_fn, cb, nullptr);
}

void ZKSession::Set(std::string_view path, std::span<const char> value,
                    int version, Callback cb) {
    EnqueueNewOp(kSet, path, nullptr, cb, [value, version] (Op* op) {
        op->value.ResetWithData(value);
        op->data_version = version;
    });
}

void ZKSession::GetChildren(std::string_view path, WatcherFn wathcer_fn, Callback cb) {
    EnqueueNewOp(kGetChildren, path, wathcer_fn, cb, nullptr);
}

void ZKSession::RemoveAllWatches(std::string_view path, Callback cb) {
    EnqueueNewOp(kRemoveAllWatches, path, nullptr, cb, nullptr);
}

void ZKSession::EnqueueNewOp(OpType type, std::string_view path,
                             WatcherFn watcher_fn, Callback cb,
                             std::function<void(Op*)> setup_fn) {
    {
        absl::MutexLock lk(&mu_);
        Op* op = op_pool_.Get();
        op->sess = this;
        op->type = type;
        op->path = std::string(path);
        op->value.Reset();
        op->create_mode = -1;
        op->data_version = -1;
        op->watcher_fn = nullptr;
        op->cb.swap(cb);
        if (watcher_fn) {
            op->watcher_fn = watcher_fn_pool_.Get();
            op->watcher_fn->swap(watcher_fn);
        }
        if (setup_fn) {
            setup_fn(op);
        }
        pending_ops_.push_back(op);
    }
    DCHECK(new_op_eventfd_ >= 0);
    PCHECK(eventfd_write(new_op_eventfd_, 1) == 0)
        << "eventfd_write failed";
}

void ZKSession::ProcessPendingOps() {
    std::vector<Op*> pending_ops;
    {
        absl::MutexLock lk(&mu_);
        pending_ops_.swap(pending_ops);
    }
    for (Op* op : pending_ops) {
        DoOp(op);
    }
}

void ZKSession::DoOp(Op* op) {
    switch (op->type) {
    case kCreate:
        ZK_DCHECK_OK(zoo_acreate(
            handle_, /* path= */ op->path.c_str(),
            /* value= */ op->value.data(), /* valuelen= */ op->value.length(),
            /* acl= */ &ZOO_OPEN_ACL_UNSAFE, /* mode= */ op->create_mode,
            &ZKSession::StringCompletionCallback, /* data= */ op));
        break;
    case kDelete:
        ZK_DCHECK_OK(zoo_adelete(
            handle_, /* path= */ op->path.c_str(), /* version= */ op->data_version,
            &ZKSession::VoidCompletionCallback, /* data= */ op));
        break;
    case kExists:
        if (op->watcher_fn != nullptr) {
            ZK_DCHECK_OK(zoo_awexists(
                handle_, /* path= */ op->path.c_str(),
                /* watcher= */ &ZKSession::WatcherCallback,
                /* watcherCtx= */ op->watcher_fn,
                &ZKSession::StatCompletionCallback, /* data= */ op));
        } else {
            ZK_DCHECK_OK(zoo_aexists(
                handle_, /* path= */ op->path.c_str(), /* watch= */ 0,
                &ZKSession::StatCompletionCallback, /* data= */ op));
        }
        break;
    case kGet:
        if (op->watcher_fn != nullptr) {
            ZK_DCHECK_OK(zoo_awget(
                handle_, /* path= */ op->path.c_str(),
                /* watcher= */ &ZKSession::WatcherCallback,
                /* watcherCtx= */ op->watcher_fn,
                &ZKSession::DataCompletionCallback, /* data= */ op));
        } else {
            ZK_DCHECK_OK(zoo_aget(
                handle_, /* path= */ op->path.c_str(), /* watch= */ 0,
                &ZKSession::DataCompletionCallback, /* data= */ op));
        }
        break;
    case kSet:
        ZK_DCHECK_OK(zoo_aset(
            handle_, /* path= */ op->path.c_str(),
            /* buffer= */ op->value.data(), /* buflen= */ op->value.length(),
            /* version= */ op->data_version,
            &ZKSession::StatCompletionCallback, /* data= */ op));
        break;
    case kGetChildren:
        if (op->watcher_fn != nullptr) {
            ZK_DCHECK_OK(zoo_awget_children(
                handle_, /* path= */ op->path.c_str(),
                /* watcher= */ &ZKSession::WatcherCallback,
                /* watcherCtx= */ op->watcher_fn,
                &ZKSession::StringsCompletionCallback, /* data= */ op));
        } else {
            ZK_DCHECK_OK(zoo_aget_children(
                handle_, /* path= */ op->path.c_str(), /* watch= */ 0,
                &ZKSession::StringsCompletionCallback, /* data= */ op));
        }
        break;
    case kRemoveAllWatches:
        ZK_DCHECK_OK(zoo_aremove_all_watches(
            handle_, /* path= */ op->path.c_str(),
            /* wtype= */ ZWATCHTYPE_ANY, /* local= */ 1,
            /* completion= */ nullptr, /* data= */ nullptr));
        OpCompleted(op, ZOK, EmptyResult());
    default:
        UNREACHABLE();
    }
}

void ZKSession::ReclaimCompletedOps() {
    DCHECK(WithinMyEventLoopThread());
    {
        absl::MutexLock lk(&mu_);
        for (Op* op : completed_ops_) {
            op_pool_.Return(op);
        }
    }
    completed_ops_.clear();
}

void ZKSession::OpCompleted(Op* op, int rc, const ZKResult& result) {
    DCHECK(WithinMyEventLoopThread());
    if (op->cb) {
        op->cb(rc, result);
    }
    completed_ops_.push_back(op);
}

bool ZKSession::WithinMyEventLoopThread() {
    return base::Thread::current() == &event_loop_thread_;
}

void ZKSession::EventLoopThreadMain() {
    absl::InlinedVector<struct pollfd, 4> pollfds;
    bool stopped = false;
    while (!stopped) {
        pollfds.clear();
        // Add stop_eventfd_ and new_op_eventfd_
        pollfds.push_back({ .fd = stop_eventfd_, .events = POLLIN, .revents = 0 });
        pollfds.push_back({ .fd = new_op_eventfd_, .events = POLLIN, .revents = 0 });

        int zk_fd;
        int zk_interest;
        struct timeval zk_timeout;
        ZK_DCHECK_OK(zookeeper_interest(handle_, &zk_fd, &zk_interest, &zk_timeout));

        short zk_fd_events = 0;
        if (zk_interest & ZOOKEEPER_READ) {
            zk_fd_events |= POLLIN;
        }
        if (zk_interest & ZOOKEEPER_WRITE) {
            zk_fd_events |= POLLOUT;
        }
        pollfds.push_back({ .fd = zk_fd, .events = zk_fd_events, .revents = 0 });

        struct timespec spec;
        TIMEVAL_TO_TIMESPEC(&zk_timeout, &spec);
        int ret = ppoll(pollfds.data(), pollfds.size(),
                        /* tmo_p= */ &spec, /* sigmask= */ nullptr);
        PCHECK(ret >= 0) << "ppoll failed";

        for (const auto& item : pollfds) {
            if (item.revents == 0) {
                continue;
            }
            CHECK_EQ(item.revents & POLLNVAL, 0)
                << fmt::format("Invalid fd {}", item.fd);
            if ((item.revents & POLLERR) != 0 || (item.revents & POLLHUP) != 0) {
                if (item.fd == zk_fd) {
                    HLOG(ERROR) << "Error happens on Zookeeper fd";
                } else {
                    HLOG(ERROR) << fmt::format("Error happens on fd {}", item.fd);
                }
                continue;
            }
            if (item.fd == zk_fd) {
                int zk_events = 0;
                if (item.revents & POLLIN) {
                    zk_events |= ZOOKEEPER_READ;
                }
                if (item.revents & POLLOUT) {
                    zk_events |= ZOOKEEPER_WRITE;
                }
                ZK_DCHECK_OK(zookeeper_process(handle_, zk_events));
            } else if (item.fd == new_op_eventfd_) {
                uint64_t value;
                PCHECK(eventfd_read(new_op_eventfd_, &value) == 0)
                    << "eventfd_read failed";
                ProcessPendingOps();
            } else if (item.fd == stop_eventfd_) {
                HLOG(INFO) << "Receive stop event";
                uint64_t value;
                PCHECK(eventfd_read(stop_eventfd_, &value) == 0)
                    << "eventfd_read failed";
                stopped = true;
                break;
            } else {
                UNREACHABLE();
            }
        }
        ReclaimCompletedOps();
    }
    state_.store(kStopped);
}

ZKResult ZKSession::EmptyResult() {
    return ZKResult {
        .value = "",
        .values = std::vector<std::string_view>(),
        .data = std::span<const char>(),
        .stat = nullptr,
    };
}

ZKResult ZKSession::StringResult(const char* string) {
    ZKResult result = EmptyResult();
    result.value = string;
    return result;
}

ZKResult ZKSession::StringsResult(const struct String_vector* strings) {
    ZKResult result = EmptyResult();
    result.values.resize(strings->count);
    for (int i = 0; i < strings->count; i++) {
        result.values[i] = std::string_view(strings->data[i]);
    }
    return result;
}

ZKResult ZKSession::DataResult(const char* data, int data_len,
                                        const struct Stat* stat) {
    ZKResult result = EmptyResult();
    result.data = std::span<const char>(data, data_len);
    result.stat = stat;
    return result;
}

ZKResult ZKSession::StatResult(const struct Stat* stat) {
    ZKResult result = EmptyResult();
    result.stat = stat;
    return result;
}

void ZKSession::WatcherCallback(zhandle_t* handle, int type, int state,
                                const char* path, void* watcher_ctx) {
    WatcherFn* fn = reinterpret_cast<WatcherFn*>(DCHECK_NOTNULL(watcher_ctx));
    (*fn)(type, state, path);
}

void ZKSession::VoidCompletionCallback(int rc, const void* data) {
    Op* op = reinterpret_cast<Op*>(const_cast<void*>(DCHECK_NOTNULL(data)));
    op->sess->OpCompleted(op, rc, EmptyResult());
}

void ZKSession::StringCompletionCallback(int rc, const char* value, const void* data) {
    Op* op = reinterpret_cast<Op*>(const_cast<void*>(DCHECK_NOTNULL(data)));
    op->sess->OpCompleted(op, rc, (rc == ZOK) ? StringResult(value) : EmptyResult());
}

void ZKSession::StringsCompletionCallback(int rc, const struct String_vector* strings,
                                          const void* data) {
    Op* op = reinterpret_cast<Op*>(const_cast<void*>(DCHECK_NOTNULL(data)));
    op->sess->OpCompleted(op, rc, (rc == ZOK) ? StringsResult(strings) : EmptyResult());
}

void ZKSession::DataCompletionCallback(int rc, const char* value, int value_len,
                                       const struct Stat* stat, const void* data) {
    Op* op = reinterpret_cast<Op*>(const_cast<void*>(DCHECK_NOTNULL(data)));
    op->sess->OpCompleted(op, rc, (rc == ZOK) ? DataResult(value, value_len, stat)
                                              : EmptyResult());
}

void ZKSession::StatCompletionCallback(int rc, const struct Stat* stat, const void* data) {
    Op* op = reinterpret_cast<Op*>(const_cast<void*>(DCHECK_NOTNULL(data)));
    op->sess->OpCompleted(op, rc, (rc == ZOK) ? StatResult(stat) : EmptyResult());
}

}  // namespace zk
}  // namespace faas
