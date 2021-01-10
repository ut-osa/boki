#pragma once

#include "base/common.h"
#include "common/zk.h"
#include "common/protocol.h"
#include "utils/appendable_buffer.h"
#include "server/io_worker.h"
#include "server/node_watcher.h"

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

    bool WithinMyEventLoopThread();

    void ForEachIOWorker(std::function<void(IOWorker* io_worker)> cb);
    IOWorker* PickIOWorkerForConnType(int conn_type);
    IOWorker* SomeIOWorker();

    void RegisterConnection(IOWorker* io_worker, ConnectionBase* connection);

    typedef std::function<void(int /* client_sockfd */)> ConnectionCallback;
    void ListenForNewConnections(int server_sockfd, ConnectionCallback cb);

    template<class T>
    T* PickConnFromCurrentIOWorker(int conn_type);

    template<class T>
    T* PickOrCreateConnFromCurrentIOWorker(int conn_type,
                                           std::function<T*(IOWorker*)> create_cb);

    // Supposed to be implemented by sub-class
    virtual void StartInternal() {}
    virtual void StopInternal() {}
    virtual void OnConnectionClose(ConnectionBase* connection) {}
    virtual void OnRemoteMessageConn(const protocol::HandshakeMessage& handshake, int sockfd);

    static int GetIngressConnTypeId(protocol::ConnType conn_type, uint16_t node_id);
    static int GetEgressHubTypeId(protocol::ConnType conn_type, uint16_t node_id);

private:
    std::string node_name_;

    int stop_eventfd_;
    int message_sockfd_;
    base::Thread event_loop_thread_;
    zk::ZKSession zk_session_;
    NodeWatcher node_watcher_;

    std::vector<std::unique_ptr<IOWorker>> io_workers_;
    absl::flat_hash_map<IOWorker*, /* fd */ int> pipes_to_io_worker_;
    absl::flat_hash_map</* fd */ int, ConnectionCallback> connection_cbs_;
    absl::flat_hash_map</* conn_type */ int, size_t> next_io_worker_id_;
    std::atomic<int> next_connection_id_;

    void SetupIOWorkers();
    void SetupMessageServer();
    void OnNewMessageConnection(int sockfd);

    void EventLoopThreadMain();
    void DoStop();
    void DoReadClosedConnection(int pipefd);
    void DoAcceptConnection(int server_sockfd);

    DISALLOW_COPY_AND_ASSIGN(ServerBase);
};

template<class T>
T* ServerBase::PickConnFromCurrentIOWorker(int conn_type) {
    IOWorker* io_worker = IOWorker::current();
    if (__FAAS_PREDICT_FALSE(io_worker == nullptr)) {
        LOG(FATAL) << "Not in thread of some IOWorker";
    }
    ConnectionBase* conn = io_worker->PickConnection(conn_type);
    if (conn == nullptr) {
        return nullptr;
    } else {
        return conn->as_ptr<T>();
    }
}

template<class T>
T* ServerBase::PickOrCreateConnFromCurrentIOWorker(int conn_type,
                                                   std::function<T*(IOWorker*)> create_cb) {
    IOWorker* io_worker = IOWorker::current();
    if (__FAAS_PREDICT_FALSE(io_worker == nullptr)) {
        LOG(FATAL) << "Not in thread of some IOWorker";
    }
    ConnectionBase* conn = io_worker->PickConnection(conn_type);
    if (conn == nullptr) {
        T* created_conn = create_cb(io_worker);
        if (created_conn != nullptr) {
            DCHECK_EQ(conn_type, created_conn->type());
            return created_conn;
        } else {
            return nullptr;
        }
    } else {
        return conn->as_ptr<T>();
    }
}

}  // namespace server
}  // namespace faas
