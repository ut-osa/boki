#include "server/server_base.h"

#include "common/flags.h"
#include "common/zk_utils.h"
#include "utils/io.h"
#include "utils/socket.h"
#include "server/constants.h"

#include <sys/types.h>
#include <sys/eventfd.h>
#include <fcntl.h>
#include <poll.h>

#define log_header_ "ServerBase: "

namespace faas {
namespace server {

ServerBase::ServerBase(std::string_view node_name)
    : state_(kCreated),
      node_name_(node_name),
      stop_eventfd_(eventfd(0, EFD_CLOEXEC)),
      message_sockfd_(-1),
      event_loop_thread_("Srv/EL",
                         absl::bind_front(&ServerBase::EventLoopThreadMain, this)),
      zk_session_(absl::GetFlag(FLAGS_zookeeper_host),
                  absl::GetFlag(FLAGS_zookeeper_root_path)),
      next_io_worker_for_pick_(0),
      next_connection_id_(0) {
    PCHECK(stop_eventfd_ >= 0) << "Failed to create eventfd";
}

ServerBase::~ServerBase() {
    PCHECK(close(stop_eventfd_) == 0) << "Failed to close eventfd";
}

bool ServerBase::journal_enabled() {
    return absl::GetFlag(FLAGS_enable_journal);
}

void ServerBase::Start() {
    DCHECK(state_.load() == kCreated);
    zk_session_.Start();
    SetupIOWorkers();
    state_.store(kBootstrapping);
    if (absl::GetFlag(FLAGS_enable_journal)) {
        SetupJournalMonitorTimers();
    }
    StartInternal();
    SetupMessageServer();
    node_watcher_.StartWatching(zk_session());
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
    zk_session_.WaitForFinish();
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

bool ServerBase::WithinMyEventLoopThread() const {
    return base::Thread::current() == &event_loop_thread_;
}

void ServerBase::ForEachIOWorker(std::function<void(IOWorker* io_worker)> cb) const {
    for (size_t i = 0; i < io_workers_.size(); i++) {
        cb(io_workers_.at(i).get());
    }
}

IOWorker* ServerBase::PickIOWorkerForConnType(int conn_type) {
    DCHECK(WithinMyEventLoopThread());
    DCHECK_GE(conn_type, 0);
    size_t idx = (next_io_worker_id_[conn_type]++) % io_workers_.size();
    return io_workers_[idx].get();
}

IOWorker* ServerBase::SomeIOWorker() const {
    size_t idx = next_io_worker_for_pick_.fetch_add(1, std::memory_order_relaxed);
    return io_workers_.at(idx % io_workers_.size()).get();
}

void ServerBase::OnRemoteMessageConn(const protocol::HandshakeMessage& handshake, int sockfd) {
    HLOG(WARNING) << "OnRemoteMessageConn supposed to be implemented by sub-class";
    close(sockfd);
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
                    HLOG_F(ERROR, "Error happens on server fd {}", item.fd);
                } else {
                    HLOG_F(FATAL, "Error happens on fd {}", item.fd);
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

void ServerBase::SetupIOWorkers() {
    DCHECK(state_.load() == kCreated);
    int num_io_workers = absl::GetFlag(FLAGS_num_io_workers);
    CHECK_GT(num_io_workers, 0);
    HLOG_F(INFO, "Start {} IO workers", num_io_workers);
    for (int i = 0; i < num_io_workers; i++) {
        auto io_worker = std::make_unique<IOWorker>(
            fmt::format("IO-{}", i), kDefaultIOWorkerBufferSize);
        int pipe_fds[2] = { -1, -1 };
        if (socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, pipe_fds) < 0) {
            PLOG(FATAL) << "socketpair failed";
        }
        io_worker->Start(pipe_fds[1]);
        pipes_to_io_worker_[io_worker.get()] = pipe_fds[0];
        io_workers_.push_back(std::move(io_worker));
    }
}

void ServerBase::SetupMessageServer() {
    DCHECK(state_.load() == kBootstrapping);
    std::string listen_iface = absl::GetFlag(FLAGS_listen_iface);
    std::string iface_ip;
    CHECK(utils::ResolveInterfaceIp(listen_iface, &iface_ip))
        << fmt::format("Failed to resolve IP for {}", listen_iface);
    uint16_t message_port;
    message_sockfd_ = utils::TcpSocketBindArbitraryPort(iface_ip, &message_port);
    CHECK(message_sockfd_ != -1)
        << fmt::format("Failed to bind on {}", iface_ip);
    CHECK(utils::SocketListen(message_sockfd_, absl::GetFlag(FLAGS_socket_listen_backlog)))
        << fmt::format("Failed to listen on {}:{}", iface_ip, message_port);
    HLOG_F(INFO, "Listen on {}:{} for message connections", iface_ip, message_port);
    ListenForNewConnections(
        message_sockfd_, absl::bind_front(&ServerBase::OnNewMessageConnection, this));
    // Save my host address to ZooKeeper for others to connect
    std::string my_addr(fmt::format("{}:{}", iface_ip, message_port));
    std::string znode_path = fmt::format("node/{}", node_name_);
    auto status = zk_utils::CreateSync(
        zk_session(), /* path= */ znode_path, /* value= */ STRING_AS_SPAN(my_addr),
        zk::ZKCreateMode::kEphemeral, nullptr);
    CHECK(status.ok()) << fmt::format("Failed to create ZooKeeper node {}: {}",
                                      znode_path, status.ToString());
}

void ServerBase::SetupJournalMonitorTimers() {
    DCHECK(state_.load() == kBootstrapping);
    absl::Time initial = absl::Now() + absl::Seconds(1);
    ForEachIOWorker([&, this] (IOWorker* io_worker) {
        Timer* timer = new Timer(
            kJournalMonitorTimerId,
            absl::bind_front(&IOWorker::JournalMonitorCallback, io_worker));
        timer->SetPeriodic(initial, absl::Seconds(1));
        RegisterConnection(io_worker, timer);
        timers_.insert(std::unique_ptr<Timer>(timer));
    });
}

void ServerBase::OnNewMessageConnection(int sockfd) {
    DCHECK(WithinMyEventLoopThread());
    protocol::HandshakeMessage handshake;
    if (!io_utils::RecvMessage(sockfd, &handshake, nullptr)) {
        HPLOG(ERROR) << "Failed to read handshake message";
        close(sockfd);
        return;
    }
    OnRemoteMessageConn(handshake, sockfd);
}

void ServerBase::RegisterConnection(IOWorker* io_worker, ConnectionBase* connection) {
    connection->set_id(next_connection_id_.fetch_add(1, std::memory_order_relaxed));
    if (io_worker->WithinMyEventLoopThread()) {
        io_worker->RegisterConnection(connection);
    } else {
        DCHECK(pipes_to_io_worker_.contains(io_worker));
        int pipe_to_worker = pipes_to_io_worker_.at(io_worker);
        ssize_t ret = write(pipe_to_worker, &connection, __FAAS_PTR_SIZE);
        if (ret < 0) {
            PLOG(FATAL) << "Write failed on pipe to IOWorker";
        } else {
            CHECK_EQ(ret, __FAAS_PTR_SIZE);
        }
    }
}

void ServerBase::ListenForNewConnections(int server_sockfd, ConnectionCallback cb) {
    DCHECK(state_.load() == kBootstrapping);
    io_utils::FdSetNonblocking(server_sockfd);
    connection_cbs_[server_sockfd] = cb;
}

void ServerBase::DoStop() {
    DCHECK(WithinMyEventLoopThread());
    if (state_.load(std::memory_order_acquire) == kStopping) {
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
        int pipefd = pipes_to_io_worker_.at(io_worker.get());
        PCHECK(close(pipefd) == 0) << "Failed to close pipe to IOWorker";
    }
    HLOG(INFO) << "All IOWorker finish";
    StopInternal();
    if (message_sockfd_ != -1) {
        PCHECK(close(message_sockfd_) == 0) << "Failed to close message server fd";
    }
    zk_session_.ScheduleStop();
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
        if ((connection->type() & kConnectionTypeMask) == kTimerTypeId) {
            Timer* timer = connection->as_ptr<Timer>();
            DCHECK(timers_.contains(timer));
            timers_.erase(timer);
        } else {
            OnConnectionClose(connection);
        }
    }
}

void ServerBase::DoAcceptConnection(int server_sockfd) {
    DCHECK(WithinMyEventLoopThread());
    DCHECK(connection_cbs_.contains(server_sockfd));
    while (true) {
        int client_sockfd = accept4(server_sockfd, nullptr, nullptr, SOCK_CLOEXEC);
        if (client_sockfd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else {
                PLOG_F(ERROR, "Accept failed on server fd {}", server_sockfd);
            }
        } else {
            connection_cbs_[server_sockfd](client_sockfd);
        }
    }
}

Timer* ServerBase::CreateTimer(int timer_type, IOWorker* io_worker, Timer::Callback cb) {
    Timer* timer = new Timer(timer_type, cb);
    RegisterConnection(io_worker, timer);
    timers_.insert(std::unique_ptr<Timer>(timer));
    return timer;
}

void ServerBase::CreatePeriodicTimer(int timer_type, absl::Duration interval,
                                     Timer::Callback cb) {
    DCHECK(state_.load() == kBootstrapping);
    absl::Time initial = absl::Now() + absl::Seconds(1);
    ForEachIOWorker([&, this] (IOWorker* io_worker) {
        Timer* timer = new Timer(timer_type, cb);
        timer->SetPeriodic(initial, interval * io_workers_.size());
        RegisterConnection(io_worker, timer);
        timers_.insert(std::unique_ptr<Timer>(timer));
        initial += interval;
    });
}

namespace {
using protocol::ConnType;
typedef std::pair<int, int> ConnTypeIdPair;
#define CONN_ID_PAIR(A, B) { k##A##IngressTypeId, k##B##EgressHubTypeId }

const absl::flat_hash_map<ConnType, ConnTypeIdPair> kConnTypeIdTable {
    { ConnType::GATEWAY_TO_ENGINE,      CONN_ID_PAIR(Gateway, Engine) },
    { ConnType::ENGINE_TO_GATEWAY,      CONN_ID_PAIR(Engine, Gateway) },
    { ConnType::SLOG_ENGINE_TO_ENGINE,  CONN_ID_PAIR(Engine, Engine) },
    { ConnType::ENGINE_TO_SEQUENCER,    CONN_ID_PAIR(Engine, Sequencer) },
    { ConnType::SEQUENCER_TO_ENGINE,    CONN_ID_PAIR(Sequencer, Engine) },
    { ConnType::SEQUENCER_TO_SEQUENCER, CONN_ID_PAIR(Sequencer, Sequencer) },
    { ConnType::ENGINE_TO_STORAGE,      CONN_ID_PAIR(Engine, Storage) },
    { ConnType::STORAGE_TO_ENGINE,      CONN_ID_PAIR(Storage, Engine) },
    { ConnType::SEQUENCER_TO_STORAGE,   CONN_ID_PAIR(Sequencer, Storage) },
    { ConnType::STORAGE_TO_SEQUENCER,   CONN_ID_PAIR(Storage, Sequencer) },
};

#undef CONN_ID_PAIR

}  // namespace

int ServerBase::GetIngressConnTypeId(protocol::ConnType conn_type, uint16_t node_id) {
    CHECK(kConnTypeIdTable.contains(conn_type));
    return kConnTypeIdTable.at(conn_type).first + node_id;
}

int ServerBase::GetEgressHubTypeId(protocol::ConnType conn_type, uint16_t node_id) {
    CHECK(kConnTypeIdTable.contains(conn_type));
    return kConnTypeIdTable.at(conn_type).second + node_id;
}

}  // namespace server
}  // namespace faas
