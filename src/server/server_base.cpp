#include "server/server_base.h"

#include "utils/io.h"

#include <sys/types.h>
#include <sys/eventfd.h>
#include <fcntl.h>
#include <poll.h>

#define log_header_ "Server: "

namespace faas {
namespace server {

ServerBase::ServerBase()
    : state_(kCreated),
      event_loop_thread_("Server/EL", absl::bind_front(&ServerBase::EventLoopThreadMain, this)),
      next_connection_id_(0) {
    stop_eventfd_ = eventfd(0, 0);
    PCHECK(stop_eventfd_ >= 0) << "Failed to create eventfd";
}

ServerBase::~ServerBase() {
    PCHECK(close(stop_eventfd_) == 0) << "Failed to close eventfd";
}

void ServerBase::Start() {
    DCHECK(state_.load() == kCreated);
    StartInternal();
    // Start thread for running event loop
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void ServerBase::ScheduleStop() {
    HLOG(INFO) << "Scheduled to stop";
    DCHECK(stop_eventfd_ >= 0);
    PCHECK(eventfd_write(stop_eventfd_, 1) == 0) << "eventfd_write failed";
}

void ServerBase::WaitForFinish() {
    DCHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

bool ServerBase::WithinMyEventLoopThread() {
    return base::Thread::current() == &event_loop_thread_;
}

void ServerBase::EventLoopThreadMain() {
    std::vector<struct pollfd> pollfds;
    // Add stop_eventfd_
    pollfds.push_back({ .fd = stop_eventfd_, .events = POLLIN, .revents = 0 });
    // Add all pipe fds to workers
    for (const auto& item : pipes_to_io_worker_) {
        pollfds.push_back({ .fd = item.second, .events = POLLIN, .revents = 0 });
    }
    // Add all fds registered with ListenForNewConnections
    for (const auto& item : connection_cbs_) {
        pollfds.push_back({ .fd = item.first, .events = POLLIN, .revents = 0 });
    }
    HLOG(INFO) << "Event loop starts";
    bool stopped = false;
    while (!stopped) {
        int ret = poll(pollfds.data(), pollfds.size(), /* timeout= */ -1);
        PCHECK(ret >= 0) << "poll failed";
        for (const auto& item : pollfds) {
            if (item.revents == 0) {
                continue;
            }
            CHECK((item.revents & POLLNVAL) == 0) << fmt::format("Invalid fd {}", item.fd);
            if ((item.revents & POLLERR) != 0 || (item.revents & POLLHUP) != 0) {
                if (connection_cbs_.contains(item.fd)) {
                    HLOG(ERROR) << fmt::format("Error happens on server fd {}", item.fd);
                } else {
                    HLOG(FATAL) << fmt::format("Error happens on fd {}", item.fd);
                }
            } else if (item.revents & POLLIN) {
                if (item.fd == stop_eventfd_) {
                    HLOG(INFO) << "Receive stop event";
                    uint64_t value;
                    PCHECK(eventfd_read(stop_eventfd_, &value) == 0) << "eventfd_read failed";
                    DoStop();
                    stopped = true;
                    break;
                } else if (connection_cbs_.contains(item.fd)) {
                    DoAcceptConnection(item.fd);
                } else {
                    DoReadClosedConnection(item.fd);
                }
            } else {
                UNREACHABLE();
            }
        }
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

IOWorker* ServerBase::CreateIOWorker(std::string_view worker_name, size_t write_buffer_size) {
    DCHECK(state_.load() == kCreated);
    auto io_worker = std::make_unique<IOWorker>(worker_name, write_buffer_size);
    int pipe_fds[2] = { -1, -1 };
    if (socketpair(AF_UNIX, SOCK_STREAM, 0, pipe_fds) < 0) {
        PLOG(FATAL) << "socketpair failed";
    }
    io_worker->Start(pipe_fds[1]);
    pipes_to_io_worker_[io_worker.get()] = pipe_fds[0];
    IOWorker* ret = io_worker.get();
    io_workers_.insert(std::move(io_worker));
    return ret;
}

void ServerBase::RegisterConnection(IOWorker* io_worker, ConnectionBase* connection) {
    connection->set_id(next_connection_id_++);
    DCHECK(pipes_to_io_worker_.contains(io_worker));
    int pipe_to_worker = pipes_to_io_worker_[io_worker];
    ssize_t ret = write(pipe_to_worker, &connection, __FAAS_PTR_SIZE);
    if (ret < 0) {
        PLOG(FATAL) << "Write failed on pipe to IOWorker";
    } else {
        CHECK_EQ(ret, __FAAS_PTR_SIZE);
    }
}

void ServerBase::ListenForNewConnections(int server_sockfd, ConnectionCallback cb) {
    DCHECK(state_.load() == kCreated);
    io_utils::FdSetNonblocking(server_sockfd);
    connection_cbs_[server_sockfd] = cb;
}

void ServerBase::DoStop() {
    DCHECK(WithinMyEventLoopThread());
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    state_.store(kStopping);
    HLOG(INFO) << "Start stopping process";
    for (const auto& io_worker : io_workers_) {
        io_worker->ScheduleStop();
    }
    for (const auto& io_worker : io_workers_) {
        io_worker->WaitForFinish();
        int pipefd = pipes_to_io_worker_[io_worker.get()];
        PCHECK(close(pipefd) == 0) << "Failed to close pipe to IOWorker";
    }
    HLOG(INFO) << "All IOWorker finish";
    StopInternal();
}

void ServerBase::DoReadClosedConnection(int pipefd) {
    DCHECK(WithinMyEventLoopThread());
    while (true) {
        ConnectionBase* connection;
        ssize_t ret = recv(pipefd, &connection, __FAAS_PTR_SIZE, MSG_DONTWAIT);
        if (ret < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                PLOG(FATAL) << "Read failed on pipe to IOWorker";
            }
        }
        CHECK_EQ(ret, __FAAS_PTR_SIZE);
        OnConnectionClose(connection);
    }
}

void ServerBase::DoAcceptConnection(int server_sockfd) {
    DCHECK(WithinMyEventLoopThread());
    DCHECK(connection_cbs_.contains(server_sockfd));
    while (true) {
        int client_sockfd = accept4(server_sockfd, nullptr, nullptr, 0);
        if (client_sockfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                PLOG(ERROR) << fmt::format("Accept failed on server fd {}", server_sockfd);
            }
        } else {
            connection_cbs_[server_sockfd](client_sockfd);
        }
    }
}

}  // namespace server
}  // namespace faas
