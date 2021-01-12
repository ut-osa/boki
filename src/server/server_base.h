#pragma once

#include "base/common.h"
#include "common/zk.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"
#include "server/node_watcher.h"
#include "server/timer.h"

namespace faas {
namespace server {

class ServerBase {
public:
    static constexpr size_t kDefaultIOWorkerBufferSize = 65536;

    explicit ServerBase(std::string_view node_name);
    virtual ~ServerBase();

    void Start();
    void ScheduleStop();
    void WaitForFinish();

protected:
    enum State { kCreated, kBootstrapping, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    zk::ZKSession* zk_session() { return &zk_session_; }
    NodeWatcher* node_watcher() { return &node_watcher_; }

    bool WithinMyEventLoopThread() const;

    void ForEachIOWorker(std::function<void(IOWorker* io_worker)> cb) const;
    IOWorker* PickIOWorkerForConnType(int conn_type);
    IOWorker* SomeIOWorker() const;

    static IOWorker* CurrentIOWorker() { return IOWorker::current(); }
    static IOWorker* CurrentIOWorkerChecked() { return DCHECK_NOTNULL(IOWorker::current()); }

    void RegisterConnection(IOWorker* io_worker, ConnectionBase* connection);

    typedef std::function<void(int /* client_sockfd */)> ConnectionCallback;
    void ListenForNewConnections(int server_sockfd, ConnectionCallback cb);

    Timer* CreateTimer(int timer_type, IOWorker* io_worker, Timer::Callback cb);
    void CreatePeriodicTimer(int timer_type, absl::Duration interval, Timer::Callback cb);

    // Supposed to be implemented by sub-class
    virtual void StartInternal() = 0;
    virtual void StopInternal() = 0;
    virtual void OnConnectionClose(ConnectionBase* connection) = 0;
    virtual void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                     int sockfd) = 0;

    static int GetIngressConnTypeId(protocol::ConnType conn_type, uint16_t node_id);
    static int GetEgressHubTypeId(protocol::ConnType conn_type, uint16_t node_id);

private:
    std::string node_name_;

    int stop_eventfd_;
    int message_sockfd_;
    base::Thread event_loop_thread_;
    zk::ZKSession zk_session_;
    NodeWatcher node_watcher_;

    mutable std::atomic<size_t> next_io_worker_for_pick_;

    std::vector<std::unique_ptr<IOWorker>> io_workers_;
    absl::flat_hash_map<IOWorker*, /* fd */ int> pipes_to_io_worker_;
    absl::flat_hash_map</* fd */ int, ConnectionCallback> connection_cbs_;
    absl::flat_hash_map</* conn_type */ int, size_t> next_io_worker_id_;
    std::atomic<int> next_connection_id_;
    absl::flat_hash_set<std::unique_ptr<Timer>> timers_;

    void SetupIOWorkers();
    void SetupMessageServer();
    void OnNewMessageConnection(int sockfd);

    void EventLoopThreadMain();
    void DoStop();
    void DoReadClosedConnection(int pipefd);
    void DoAcceptConnection(int server_sockfd);

    DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

}  // namespace server
}  // namespace faas
