#include "server/io_worker.h"

#include "common/flags.h"
#include "utils/fs.h"
#include "server/constants.h"
#include "server/server_base.h"
#include "server/journal.h"

#include <sys/eventfd.h>

#define LOG_HEADER log_header_

namespace faas {
namespace server {

IOUring* ConnectionBase::current_io_uring() {
    return IOWorker::current()->io_uring();
}

thread_local IOWorker* IOWorker::current_ = nullptr;

IOWorker::IOWorker(std::string_view worker_name)
    : worker_name_(worker_name),
      state_(kCreated),
      io_uring_(),
      server_(nullptr),
      eventfd_(-1),
      pipe_to_server_fd_(-1),
      log_header_(fmt::format("{}: ", worker_name)),
      event_loop_thread_(fmt::format("{}/EL", worker_name),
                         absl::bind_front(&IOWorker::EventLoopThreadMain, this)),
      write_buffer_pool_(fmt::format("{}_Write", worker_name), kWriteBufferSize),
      connections_on_closing_(0),
      next_journal_file_id_(0),
      current_journal_file_(nullptr),
      num_created_files_(0),
      num_closed_files_(0),
      total_bytes_(0),
      total_records_(0),
      journal_record_size_stat_(stat::StatisticsCollector<uint32_t>::StandardReportCallback(
          fmt::format("journal_record_size[{}]", worker_name)), "journal"),
      journal_append_latency_stat_(stat::StatisticsCollector<int32_t>::StandardReportCallback(
          fmt::format("journal_append_latency[{}]", worker_name)), "journal") {}

IOWorker::~IOWorker() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    DCHECK(connections_.empty());
    DCHECK_EQ(connections_on_closing_, 0);
}

void IOWorker::Start(ServerBase* server, int pipe_to_server_fd, bool enable_journal) {
    DCHECK(state_.load() == kCreated);
    server_ = server;
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
    if (enable_journal) {
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
    if (WithinMyEventLoopThread()) {
        ScheduleIdleFunction(owner, fn);
        return;
    }
    bool need_notify = false;
    {
        absl::MutexLock lk(&scheduled_function_mu_);
        if (scheduled_functions_.empty()) {
            need_notify = true;
        }
        scheduled_functions_.push_back(ScheduledFunction {
            .owner_id = (owner == nullptr) ? -1 : owner->id(),
            .fn = fn
        });
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
    while (!idle_functions_.empty()) {
        ScheduledFunction fn = std::move(idle_functions_.front());
        idle_functions_.pop_front();
        InvokeFunction(fn);
    }
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

void IOWorker::JournalAppend(uint16_t type,
                             std::initializer_list<std::span<const char>> payload_vec,
                             JournalAppendCallback cb) {
    DCHECK(WithinMyEventLoopThread());
    if (current_journal_file_ == nullptr) {
        HLOG(FATAL) << "Journal not enabled!";
    }
    size_t record_size = current_journal_file_->AppendRecord(type, payload_vec, std::move(cb));
    total_bytes_.fetch_add(record_size, std::memory_order_relaxed);
    total_records_.fetch_add(1, std::memory_order_relaxed);
}

void IOWorker::JournalMonitorCallback() {
    DCHECK(WithinMyEventLoopThread());
    if (current_journal_file_ == nullptr) {
        HLOG(FATAL) << "Journal not enabled!";
    }
    if (current_journal_file_->ReachLimit()) {
        JournalFile* old_journal_file = current_journal_file_;
        current_journal_file_ = CreateNewJournalFile();
        old_journal_file->Finalize();
    }
}

JournalFile* IOWorker::CreateNewJournalFile() {
    int file_id = server_->NextJournalFileID();
    JournalFile* journal_file = new JournalFile(this, file_id);
    journal_files_[file_id] = absl::WrapUnique(journal_file);
    server_->OnJournalFileCreated(journal_file);
    num_created_files_.fetch_add(1, std::memory_order_relaxed);
    return journal_file;
}

void IOWorker::OnJournalFileClosed(JournalFile* file) {
    num_closed_files_.fetch_add(1, std::memory_order_relaxed);
    server_->OnJournalFileClosed(file);
}

void IOWorker::OnJournalFileRemoved(JournalFile* file) {
    DCHECK(journal_files_.contains(file->file_id()));
    journal_files_.erase(file->file_id());
}

void IOWorker::OnJournalRecordAppended(const protocol::JournalRecordHeader& hdr) {
    DCHECK(WithinMyEventLoopThread());
    journal_record_size_stat_.AddSample(hdr.record_size);
    journal_append_latency_stat_.AddSample(
        gsl::narrow_cast<int32_t>((GetRealtimeNanoTimestamp() - hdr.timestamp) / 1000));
}

void IOWorker::AggregateJournalStat(JournalStat* stat) {
    stat->num_created_files += num_created_files_.load(std::memory_order_relaxed);
    stat->num_closed_files  += num_closed_files_.load(std::memory_order_relaxed);
    stat->total_bytes       += total_bytes_.load(std::memory_order_relaxed);
    stat->total_records     += total_records_.load(std::memory_order_relaxed);
}

}  // namespace server
}  // namespace faas
