#include "engine/io_worker.h"

#include <sys/eventfd.h>

namespace faas {
namespace engine {

IOUring* ConnectionBase::current_io_uring() {
    return IOWorker::current()->io_uring();
}

thread_local IOWorker* IOWorker::current_ = nullptr;

IOWorker::IOWorker(std::string_view worker_name, size_t write_buffer_size)
    : worker_name_(worker_name), state_(kCreated), io_uring_(),
      eventfd_(-1), pipe_to_server_fd_(-1),
      log_header_(fmt::format("{}: ", worker_name)),
      event_loop_thread_(fmt::format("{}/EL", worker_name),
                         absl::bind_front(&IOWorker::EventLoopThreadMain, this)),
      write_buffer_pool_(fmt::format("{}_Write", worker_name), write_buffer_size),
      connections_on_closing_(0),
      message_counter_(stat::Counter::StandardReportCallback(
          fmt::format("{} message_counter", worker_name))),
      message_processing_time_counter_(stat::Counter::StandardReportCallback(
          fmt::format("{} message_processing_time", worker_name))) {
}

IOWorker::~IOWorker() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    DCHECK(connections_.empty());
    DCHECK_EQ(connections_on_closing_, 0);
}

void IOWorker::Start(int pipe_to_server_fd) {
    DCHECK(state_.load() == kCreated);
    // Setup eventfd for scheduling functions
    eventfd_ = eventfd(0, 0);
    PCHECK(eventfd_ >= 0) << "Failed to create eventfd";
    io_uring_.PrepareBuffers(kOctaBufGroup, 8);
    URING_DCHECK_OK(io_uring_.RegisterFd(eventfd_));
    URING_DCHECK_OK(io_uring_.StartRead(
        eventfd_, kOctaBufGroup,
        [this] (int status, std::span<const char> data) -> bool {
            if (state_.load(std::memory_order_consume) != kRunning) {
                return false;
            }
            PCHECK(status == 0);
            HVLOG(1) << "eventfd triggered";
            RunScheduledFunctions();
            return true;
        }
    ));
    // Setup pipe to server for receiving connections
    pipe_to_server_fd_ = pipe_to_server_fd;
    URING_DCHECK_OK(io_uring_.RegisterFd(pipe_to_server_fd_));
    URING_DCHECK_OK(io_uring_.StartRecv(
        pipe_to_server_fd_, kOctaBufGroup,
        [this] (int status, std::span<const char> data) -> bool {
            PCHECK(status == 0);
            if (data.size() == 0) {
                HLOG(INFO) << "Pipe to server closed";
                return false;
            }
            CHECK_EQ(data.size(), static_cast<size_t>(__FAAS_PTR_SIZE));
            ConnectionBase* connection;
            memcpy(&connection, data.data(), __FAAS_PTR_SIZE);
            OnNewConnection(connection);
            return true;
        }
    ));
    // Start event loop thread
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void IOWorker::ScheduleStop() {
    ScheduleFunction(nullptr, [this] { StopInternal(); });
}

void IOWorker::WaitForFinish() {
    DCHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
}

bool IOWorker::WithinMyEventLoopThread() {
    return base::Thread::current() == &event_loop_thread_;
}

void IOWorker::OnConnectionClose(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    DCHECK(connections_.contains(connection->id()));
    connections_.erase(connection->id());
    if (connection->type() >= 0) {
        DCHECK(connections_by_type_[connection->type()].contains(connection->id()));
        connections_by_type_[connection->type()].erase(connection->id());
        if (connections_for_pick_.contains(connection->type())) {
            connections_for_pick_.erase(connection->type());
        }
        HLOG(INFO) << fmt::format("One connection of type {0} closed, total of type {0} is {1}",
                                  connection->type(),
                                  connections_by_type_[connection->type()].size());
    }
    DCHECK(pipe_to_server_fd_ >= -1);
    char* buf = connection->pipe_write_buf_for_transfer();
    memcpy(buf, &connection, __FAAS_PTR_SIZE);
    std::span<const char> data(buf, __FAAS_PTR_SIZE);
    connections_on_closing_++;
    URING_DCHECK_OK(io_uring_.Write(
        pipe_to_server_fd_, data,
        [this] (int status, size_t nwrite) {
            PCHECK(status == 0);
            CHECK_EQ(nwrite, static_cast<size_t>(__FAAS_PTR_SIZE));
            DCHECK_GT(connections_on_closing_, 0);
            connections_on_closing_--;
            if (state_.load(std::memory_order_consume) == kStopping
                    && connections_.empty()
                    && connections_on_closing_ == 0) {
                // We have returned all Connection objects to Server
                CloseWorkerFds();
            }
        }
    ));
}

void IOWorker::NewWriteBuffer(std::span<char>* buf) {
    DCHECK(WithinMyEventLoopThread());
    write_buffer_pool_.Get(buf);
}

void IOWorker::ReturnWriteBuffer(std::span<char> buf) {
    DCHECK(WithinMyEventLoopThread());
    write_buffer_pool_.Return(buf);
}

ConnectionBase* IOWorker::PickConnection(int type) {
    DCHECK(WithinMyEventLoopThread());
    if (!connections_by_type_.contains(type) || connections_by_type_[type].empty()) {
        return nullptr;
    }
    size_t n_conn = connections_by_type_[type].size();
    if (!connections_for_pick_.contains(type)) {
        std::vector<ConnectionBase*> conns;
        for (int id : connections_by_type_[type]) {
            DCHECK(connections_.contains(id));
            conns.push_back(connections_[id]);
        }
        connections_for_pick_[type] = std::move(conns);
    } else {
        DCHECK_EQ(connections_for_pick_[type].size(), n_conn);
    }
    size_t idx = connections_for_pick_rr_[type] % n_conn;
    connections_for_pick_rr_[type]++;
    return connections_for_pick_[type][idx];
}

void IOWorker::EventLoopThreadMain() {
    current_ = this;
    HLOG(INFO) << "Event loop starts";
    int inflight_ops;
    do {
        io_uring_.EventLoopRunOnce(&inflight_ops);
    } while (inflight_ops > 0);
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

void IOWorker::ScheduleFunction(ConnectionBase* owner, std::function<void()> fn) {
    if (state_.load(std::memory_order_consume) != kRunning) {
        HLOG(WARNING) << "Cannot schedule function in non-running state, will ignore it";
        return;
    }
    if (WithinMyEventLoopThread()) {
        fn();
        return;
    }
    std::unique_ptr<ScheduledFunction> function = std::make_unique<ScheduledFunction>();
    function->owner_id = (owner == nullptr) ? -1 : owner->id();
    function->fn = fn;
    absl::MutexLock lk(&scheduled_function_mu_);
    scheduled_functions_.push_back(std::move(function));
    DCHECK(eventfd_ >= 0);
    PCHECK(eventfd_write(eventfd_, 1) == 0) << "eventfd_write failed";
}

void IOWorker::OnNewConnection(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    connection->Start(this);
    DCHECK(connection->id() >= 0);
    DCHECK(!connections_.contains(connection->id()));
    connections_[connection->id()] = connection;
    if (connection->type() >= 0) {
        connections_by_type_[connection->type()].insert(connection->id());
        if (connections_for_pick_.contains(connection->type())) {
            connections_for_pick_[connection->type()].push_back(connection);
        }
        HLOG(INFO) << fmt::format("New connection of type {0}, total of type {0} is {1}",
                                  connection->type(),
                                  connections_by_type_[connection->type()].size());
    }
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Receive new connection in stopping state, will close it directly";
        connection->ScheduleClose();
    }
}

void IOWorker::RunScheduledFunctions() {
    DCHECK(WithinMyEventLoopThread());
    if (state_.load(std::memory_order_consume) != kRunning) {
        return;
    }
    absl::InlinedVector<std::unique_ptr<ScheduledFunction>, 16> functions;
    {
        absl::MutexLock lk(&scheduled_function_mu_);
        functions = std::move(scheduled_functions_);
        scheduled_functions_.clear();
    }
    for (const auto& function : functions) {
        if (function->owner_id < 0 || connections_.contains(function->owner_id)) {
            function->fn();
        } else {
            HLOG(WARNING) << "Owner connection has closed";
        }
    }
}

void IOWorker::StopInternal() {
    DCHECK(WithinMyEventLoopThread());
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    HLOG(INFO) << "Start stopping process";
    state_.store(kStopping);
    if (connections_.empty() && connections_on_closing_ == 0) {
        CloseWorkerFds();
    } else {
        std::vector<ConnectionBase*> running_connections;
        for (const auto& entry : connections_) {
            running_connections.push_back(entry.second);
        }
        for (ConnectionBase* connection : running_connections) {
            connection->ScheduleClose();
        }
    }
}

void IOWorker::CloseWorkerFds() {
    HLOG(INFO) << "Close worker fds";
    io_uring_.StopReadOrRecv(eventfd_);
    URING_DCHECK_OK(io_uring_.Close(eventfd_, [this] () {
        URING_DCHECK_OK(io_uring_.UnregisterFd(eventfd_));
        eventfd_ = -1;
    }));
    io_uring_.StopReadOrRecv(pipe_to_server_fd_);
    URING_DCHECK_OK(io_uring_.Close(pipe_to_server_fd_, [this] () {
        URING_DCHECK_OK(io_uring_.UnregisterFd(pipe_to_server_fd_));
        pipe_to_server_fd_ = -1;
    }));
}

}  // namespace engine
}  // namespace faas
