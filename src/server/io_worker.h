#pragma once

#include "base/common.h"
#include "base/thread.h"
#include "utils/buffer_pool.h"
#include "utils/round_robin_set.h"
#include "server/io_uring.h"

namespace faas {
namespace server {

class IOWorker;

class ConnectionBase : public std::enable_shared_from_this<ConnectionBase> {
public:
    explicit ConnectionBase(int type = -1) : type_(type), id_(-1) {}
    virtual ~ConnectionBase() {}

    int type() const { return type_; }
    int id() const { return id_; }

    template<class T>
    T* as_ptr() { return static_cast<T*>(this); }
    std::shared_ptr<ConnectionBase> ref_self() { return shared_from_this(); }

    virtual void Start(IOWorker* io_worker) = 0;
    virtual void ScheduleClose() = 0;

    // Only used for transferring connection from Server to IOWorker
    void set_id(int id) { id_ = id; }
    char* pipe_write_buf_for_transfer() { return pipe_write_buf_for_transfer_; }

protected:
    int type_;
    int id_;

    static IOUring* current_io_uring();

private:
    char pipe_write_buf_for_transfer_[__FAAS_PTR_SIZE];

    DISALLOW_COPY_AND_ASSIGN(ConnectionBase);
};

class IOWorker final {
public:
    IOWorker(std::string_view worker_name, size_t write_buffer_size);
    ~IOWorker();

    std::string_view worker_name() const { return worker_name_; }
    IOUring* io_uring() { return &io_uring_; }

    // Return current IOWorker within event loop thread
    static IOWorker* current() { return current_; }

    void Start(int pipe_to_server_fd);
    void ScheduleStop();
    void WaitForFinish();
    bool WithinMyEventLoopThread();

    void RegisterConnection(ConnectionBase* connection);

    // Called by Connection for ONLY once
    void OnConnectionClose(ConnectionBase* connection);

    // Can only be called from this worker's event loop
    void NewWriteBuffer(std::span<char>* buf);
    void ReturnWriteBuffer(std::span<char> buf);
    // Pick a connection of given type managed by this IOWorker
    ConnectionBase* PickConnection(int type);

    // Schedule a function to run on this IO worker's event loop
    // thread. It can be called safely from other threads.
    // When the function is ready to run, IO worker will check if its
    // owner connection is still active, and will not run the function
    // if it is closed.
    void ScheduleFunction(ConnectionBase* owner, std::function<void()> fn);

    // Idle functions will be invoked at the end of each event loop iteration.
    void ScheduleIdleFunction(ConnectionBase* owner, std::function<void()> fn);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };

    std::string worker_name_;
    std::atomic<State> state_;
    IOUring io_uring_;
    static thread_local IOWorker* current_;

    int eventfd_;
    int pipe_to_server_fd_;

    std::string log_header_;

    base::Thread event_loop_thread_;
    absl::flat_hash_map</* id */ int, ConnectionBase*> connections_;
    absl::flat_hash_map</* type */ int,
                        std::unique_ptr<utils::RoundRobinSet</* id */ int>>> connections_by_type_;
    utils::BufferPool write_buffer_pool_;
    int connections_on_closing_;

    struct ScheduledFunction {
        int owner_id;
        std::function<void()> fn;
    };
    absl::Mutex scheduled_function_mu_;
    absl::InlinedVector<ScheduledFunction, 16>
        scheduled_functions_ ABSL_GUARDED_BY(scheduled_function_mu_);
    absl::InlinedVector<ScheduledFunction, 16> idle_functions_;

    void EventLoopThreadMain();
    void RunScheduledFunctions();
    void RunIdleFunctions();
    void InvokeFunction(const ScheduledFunction& function);
    void StopInternal();
    void CloseWorkerFds();

    DISALLOW_COPY_AND_ASSIGN(IOWorker);
};

}  // namespace server
}  // namespace faas
