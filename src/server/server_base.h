#pragma once

#include "base/common.h"
#include "common/zk.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"

namespace faas {
namespace server {

class ServerBase {
public:
    static constexpr size_t kDefaultIOWorkerBufferSize = 65536;

    ServerBase();
    virtual ~ServerBase();

    void Start();
    void ScheduleStop();
    void WaitForFinish();

protected:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    zk::ZKSession* zk_session() { return &zk_session_; }

    bool WithinMyEventLoopThread();

    IOWorker* CreateIOWorker(std::string_view worker_name,
                             size_t write_buffer_size = kDefaultIOWorkerBufferSize);
    void RegisterConnection(IOWorker* io_worker, ConnectionBase* connection);

    typedef std::function<void(int /* client_sockfd */)> ConnectionCallback;
    void ListenForNewConnections(int server_sockfd, ConnectionCallback cb);

    // Supposed to be implemented by sub-class
    virtual void StartInternal() {}
    virtual void StopInternal() {}
    virtual void OnConnectionClose(ConnectionBase* connection) {}

private:
    int stop_eventfd_;
    base::Thread event_loop_thread_;
    zk::ZKSession zk_session_;

    absl::flat_hash_set<std::unique_ptr<IOWorker>> io_workers_;
    absl::flat_hash_map<IOWorker*, /* fd */ int> pipes_to_io_worker_;
    absl::flat_hash_map</* fd */ int, ConnectionCallback> connection_cbs_;
    int next_connection_id_;

    void EventLoopThreadMain();
    void DoStop();
    void DoReadClosedConnection(int pipefd);
    void DoAcceptConnection(int server_sockfd);

    DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

}  // namespace server
}  // namespace faas
