#include "server/io_worker.h"

#include "common/flags.h"
#include "common/protocol.h"
#include "server/constants.h"
#include "utils/fs.h"

#include <fcntl.h>
#include <sys/eventfd.h>

ABSL_FLAG(bool, journal_file_openflag_dsync, false, "");

namespace faas {
namespace server {

IOUring* ConnectionBase::current_io_uring() {
    return IOWorker::current()->io_uring();
}

bool ConnectionBase::journal_enabled() {
    return absl::GetFlag(FLAGS_enable_journal);
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
      journal_buffer_pool_(fmt::format("{}_Journal", worker_name), kJournalBufSize),
      next_journal_file_id_(0),
      current_journal_file_(nullptr) {}

IOWorker::~IOWorker() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    DCHECK(connections_.empty());
    DCHECK_EQ(connections_on_closing_, 0);
}

void IOWorker::Start(int pipe_to_server_fd) {
    DCHECK(state_.load() == kCreated);
    // Setup eventfd for scheduling functions
    eventfd_ = eventfd(0, EFD_CLOEXEC);
    PCHECK(eventfd_ >= 0) << "Failed to create eventfd";
    io_uring_.PrepareBuffers(kOctaBufGroup, 8);
    URING_DCHECK_OK(io_uring_.RegisterFd(eventfd_));
    URING_DCHECK_OK(io_uring_.StartRead(
        eventfd_, kOctaBufGroup,
        [this] (int status, std::span<const char> data) -> bool {
            if (state_.load(std::memory_order_acquire) != kRunning) {
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
            RegisterConnection(connection);
            return true;
        }
    ));
    if (absl::GetFlag(FLAGS_enable_journal)) {
        current_journal_file_ = CreateNewJournalFile();
    }
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

void IOWorker::RegisterConnection(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    connection->Start(this);
    DCHECK(connection->id() >= 0);
    DCHECK(!connections_.contains(connection->id()));
    connections_[connection->id()] = connection;
    int conn_type = connection->type();
    if (conn_type >= 0) {
        if (!connections_by_type_.contains(conn_type)) {
            connections_by_type_[conn_type].reset(new utils::RoundRobinSet<int>());
        }
        connections_by_type_[conn_type]->Add(connection->id());
        HLOG_F(INFO, "New connection of type {0}, total of type {0} is {1}",
               conn_type, connections_by_type_[conn_type]->size());
    }
    if (state_.load(std::memory_order_acquire) == kStopping) {
        HLOG(WARNING) << "Receive new connection in stopping state, will close it directly";
        connection->ScheduleClose();
    }
}

void IOWorker::OnConnectionClose(ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    DCHECK(connections_.contains(connection->id()));
    connections_.erase(connection->id());
    int conn_type = connection->type();
    if (conn_type >= 0) {
        DCHECK(connections_by_type_.contains(conn_type));
        connections_by_type_[conn_type]->Remove(connection->id());
        HLOG_F(INFO, "One connection of type {0} closed, total of type {0} is {1}",
               conn_type, connections_by_type_[conn_type]->size());
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
            if (state_.load(std::memory_order_acquire) == kStopping
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
    if (!connections_by_type_.contains(type)) {
        return nullptr;
    }
    int conn_id;
    if (!connections_by_type_[type]->PickNext(&conn_id)) {
        return nullptr;
    }
    DCHECK(connections_.contains(conn_id));
    return connections_[conn_id];
}

void IOWorker::EventLoopThreadMain() {
    current_ = this;
    HLOG(INFO) << "Event loop starts";
    size_t inflight_ops;
    do {
        io_uring_.EventLoopRunOnce(&inflight_ops);
        RunIdleFunctions();
    } while (inflight_ops > 0);
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

void IOWorker::ScheduleFunction(ConnectionBase* owner, std::function<void()> fn) {
    if (state_.load(std::memory_order_acquire) != kRunning) {
        HLOG(WARNING) << "Cannot schedule function in non-running state, will ignore it";
        return;
    }
    ScheduledFunction function = {
        .owner_id = (owner == nullptr) ? -1 : owner->id(),
        .fn = fn
    };
    if (WithinMyEventLoopThread()) {
        InvokeFunction(function);
        return;
    }
    bool need_notify = false;
    {
        absl::MutexLock lk(&scheduled_function_mu_);
        if (scheduled_functions_.empty()) {
            need_notify = true;
        }
        scheduled_functions_.push_back(std::move(function));
    }
    if (need_notify) {
        DCHECK(eventfd_ >= 0);
        PCHECK(eventfd_write(eventfd_, 1) == 0) << "eventfd_write failed";
    }
}

void IOWorker::ScheduleIdleFunction(ConnectionBase* owner, std::function<void()> fn) {
    DCHECK(WithinMyEventLoopThread());
    if (state_.load(std::memory_order_acquire) != kRunning) {
        HLOG(WARNING) << "Cannot schedule function in non-running state, will ignore it";
        return;
    }
    idle_functions_.push_back(ScheduledFunction {
        .owner_id = (owner == nullptr) ? -1 : owner->id(),
        .fn = fn
    });
}

void IOWorker::RunScheduledFunctions() {
    DCHECK(WithinMyEventLoopThread());
    if (state_.load(std::memory_order_acquire) != kRunning) {
        return;
    }
    absl::InlinedVector<ScheduledFunction, 16> functions;
    {
        absl::MutexLock lk(&scheduled_function_mu_);
        functions = std::move(scheduled_functions_);
        scheduled_functions_.clear();
    }
    for (const ScheduledFunction& function : functions) {
        InvokeFunction(function);
    }
}

void IOWorker::RunIdleFunctions() {
    DCHECK(WithinMyEventLoopThread());
    if (state_.load(std::memory_order_acquire) != kRunning) {
        return;
    }
    for (const ScheduledFunction& function : idle_functions_) {
        InvokeFunction(function);
    }
    idle_functions_.clear();
}

void IOWorker::InvokeFunction(const ScheduledFunction& function) {
    DCHECK(WithinMyEventLoopThread());
    if (function.owner_id < 0 || connections_.contains(function.owner_id)) {
        function.fn();
    } else {
        HLOG(WARNING) << "Owner connection has closed";
    }
}

void IOWorker::StopInternal() {
    DCHECK(WithinMyEventLoopThread());
    if (state_.load(std::memory_order_acquire) == kStopping) {
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
    URING_DCHECK_OK(io_uring_.Close(eventfd_, [this] () {
        eventfd_ = -1;
    }));
    URING_DCHECK_OK(io_uring_.Close(pipe_to_server_fd_, [this] () {
        pipe_to_server_fd_ = -1;
    }));
}

void IOWorker::JournalAppend(uint16_t type, std::span<const char> payload,
                             std::function<void()> cb) {
    DCHECK(WithinMyEventLoopThread());
    if (current_journal_file_ == nullptr) {
        HLOG(FATAL) << "Journal not enabled!";
    }
    std::span<char> buf;
    journal_buffer_pool_.Get(&buf);
    size_t write_size = sizeof(protocol::JournalRecordHeader) + payload.size();
    CHECK_LE(write_size, buf.size());
    protocol::JournalRecordHeader hdr = {
        .type         = type,
        .payload_size = gsl::narrow_cast<uint16_t>(payload.size()),
        .timestamp    = GetRealtimeNanoTimestamp(),
    };
    memcpy(buf.data(), &hdr, sizeof(protocol::JournalRecordHeader));
    memcpy(buf.data() + sizeof(protocol::JournalRecordHeader),
           payload.data(), payload.size());
    int fd = current_journal_file_->fd;
    DCHECK(fd != -1);
    current_journal_file_->size += write_size;
    HVLOG_F(1, "Append to journal file (fd {}): type={}, size={}",
            fd, type, write_size);
    URING_DCHECK_OK(io_uring_.Write(
        fd, std::span<const char>(buf.data(), write_size),
        [this, buf, write_size, cb] (int status, size_t nwrite) {
            journal_buffer_pool_.Return(buf);
            if (status != 0) {
                HPLOG(FATAL) << "Failed to append journal";
            } else if (nwrite < write_size) {
                HPLOG_F(FATAL, "Partial write occurs: nwrite={}, expect={}",
                        nwrite, write_size);
            }
            cb();
        }
    ));
}

void IOWorker::JournalMonitorCallback() {
    DCHECK(WithinMyEventLoopThread());
    if (current_journal_file_ == nullptr) {
        HLOG(FATAL) << "Journal not enabled!";
    }
    size_t size_cap = absl::GetFlag(FLAGS_journal_file_max_size_mb) * 1024 * 1024;
    if (current_journal_file_->size <= size_cap) {
        return;
    }
    JournalFile* old_journal_file = current_journal_file_;
    current_journal_file_ = CreateNewJournalFile();
    int fd = old_journal_file->fd;
    DCHECK(fd != -1);
    HLOG_F(INFO, "Going to close journal file: {} (fd {})",
           old_journal_file->file_path, fd);
    URING_DCHECK_OK(io_uring_.Close(fd, [this, old_journal_file] () {
        HLOG(INFO) << "Journal file closed: " << old_journal_file->file_path;
        old_journal_file->fd = -1;
        RemoveExtraJournalFiles();
    }));
    RemoveExtraJournalFiles();
}

IOWorker::JournalFile* IOWorker::CreateNewJournalFile() {
    std::string file_path = fs_utils::JoinPath(
        absl::GetFlag(FLAGS_journal_save_path),
        fmt::format("{}.{}", worker_name_, next_journal_file_id_++));
    if (auto fd = fs_utils::Create(file_path); fd) {
        PCHECK(close(fd.value()) == 0) << "Failed to close file";
    } else {
        LOG(FATAL) << "Failed to create file " << file_path;
    }
    int flags = O_WRONLY | O_APPEND | O_NONBLOCK;
    if (absl::GetFlag(FLAGS_journal_file_openflag_dsync)) {
        flags |= O_DSYNC;
    }
    auto fd = fs_utils::Open(file_path, flags);
    if (!fd) {
        LOG(FATAL) << "Failed to open file " << file_path;
    }
    HLOG_F(INFO, "Create journal file: {} (fd {})", file_path, fd.value());
    JournalFile* journal_file = new JournalFile {
        .file_path = std::move(file_path),
        .fd        = fd.value(),
        .size      = 0,
    };
    journal_files_.emplace_back(journal_file);
    URING_DCHECK_OK(io_uring_.RegisterFd(journal_file->fd));
    return journal_file;
}

void IOWorker::RemoveExtraJournalFiles() {
    DCHECK(WithinMyEventLoopThread());
    while (journal_files_.size() > absl::GetFlag(FLAGS_journal_cap_per_worker)) {
        JournalFile* journal_file = journal_files_.front().get();
        if (journal_file->fd >= 0) {
            break;
        }
        bool success = fs_utils::Remove(journal_file->file_path);
        if (!success) {
            LOG(FATAL) << "Failed to delete file " << journal_file->file_path;
        }
        journal_files_.pop_front();
    }
}

}  // namespace server
}  // namespace faas
