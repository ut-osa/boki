#include "log/storage.h"

#include "common/flags.h"
#include "server/constants.h"

#define log_header_ "Storage: "

namespace faas {
namespace log {

using protocol::SharedLogMessage;

Storage::Storage(uint16_t node_id)
    : ServerBase(fmt::format("storage_{}", node_id)),
      node_id_(node_id) {}

Storage::~Storage() {}

void Storage::StartInternal() {
    SetupRocksDB();
    SetupZKWatchers();
}

void Storage::StopInternal() {}

void Storage::SetupRocksDB() {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.compression = rocksdb::kZSTD;
    rocksdb::DB* db;
    HLOG(INFO) << fmt::format("Open RocksDB at path {}", db_path_);
    auto status = rocksdb::DB::Open(options, db_path_, &db);
    if (!status.ok()) {
        HLOG(FATAL) << "RocksDB open failed: " << status.ToString();
    }
    db_.reset(db);
}

void Storage::SetupZKWatchers() {
    node_watcher_.StartWatching(zk_session());
    view_watcher_.SetViewCreatedCallback(
        absl::bind_front(&Storage::OnViewCreated, this));
    view_watcher_.SetViewFinalizedCallback(
        absl::bind_front(&Storage::OnViewFinalized, this));
    view_watcher_.StartWatching(zk_session());
}

void Storage::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
}

void Storage::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
}

void Storage::OnRecvSequencerMessage(uint16_t src_node_id, const SharedLogMessage& message,
                                     std::span<const char> payload) {

}

void Storage::OnRecvEngineMessage(uint16_t src_node_id, const SharedLogMessage& message,
                                  std::span<const char> payload) {

}

bool Storage::SendSequencerMessage(uint16_t dst_node_id,
                                   const SharedLogMessage& message,
                                   std::span<const char> payload) {
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_SEQUENCER,
                                dst_node_id, message, payload);
}

bool Storage::SendEngineMessage(uint16_t dst_node_id,
                                const SharedLogMessage& message,
                                std::span<const char> payload) {
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE,
                                dst_node_id, message, payload);
}

void Storage::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                     const SharedLogMessage& message,
                                     std::span<const char> payload) {
    switch (conn_type & kConnectionTypeMask) {
    case kSequencerIngressTypeId:
        OnRecvSequencerMessage(src_node_id, message, payload);
        break;
    case kEngineIngressTypeId:
        OnRecvEngineMessage(src_node_id, message, payload);
        break;
    default:
        UNREACHABLE();
    }
}

bool Storage::SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                                   const SharedLogMessage& message,
                                   std::span<const char> payload) {
    server::EgressHub* hub = PickOrCreateConnFromCurrentIOWorker<server::EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        absl::bind_front(&Storage::CreateEgressHub, this, conn_type, dst_node_id));
    if (hub == nullptr) {
        return false;
    }
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(SharedLogMessage));
    hub->SendMessage(data, payload);
    return true;
}

void Storage::OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                  int sockfd) {
    protocol::ConnType type = static_cast<protocol::ConnType>(handshake.conn_type);
    uint16_t src_node_id = handshake.src_node_id;

    switch (server::NodeWatcher::GetSrcNodeType(type)) {
    case server::NodeWatcher::kEngineNode:
        break;
    case server::NodeWatcher::kSequencerNode:
        break;
    default:
        HLOG(ERROR) << "Invalid connection type: " << handshake.conn_type;
        close(sockfd);
        return;
    }

    int conn_type_id = ServerBase::GetIngressConnTypeId(type, src_node_id);
    auto connection = std::make_unique<server::IngressConnection>(
        conn_type_id, sockfd, sizeof(SharedLogMessage));
    connection->SetMessageFullSizeCallback(
        &server::IngressConnection::SharedLogMessageFullSizeCallback);
    connection->SetNewMessageCallback(
        server::IngressConnection::BuildNewSharedLogMessageCallback(
            absl::bind_front(&Storage::OnRecvSharedLogMessage, this,
                             conn_type_id, src_node_id)));
    RegisterConnection(PickIOWorkerForConnType(conn_type_id), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!ingress_conns_.contains(connection->id()));
    ingress_conns_[connection->id()] = std::move(connection);
}

void Storage::OnConnectionClose(server::ConnectionBase* connection) {
    DCHECK(WithinMyEventLoopThread());
    switch (connection->type() & kConnectionTypeMask) {
    case kSequencerIngressTypeId:
        ABSL_FALLTHROUGH_INTENDED;
    case kEngineIngressTypeId:
        DCHECK(ingress_conns_.contains(connection->id()));
        ingress_conns_.erase(connection->id());
        break;
    case kSequencerEgressHubTypeId:
        ABSL_FALLTHROUGH_INTENDED;
    case kEngineEgressHubTypeId:
        {
            absl::MutexLock lk(&conn_mu_);
            DCHECK(!egress_hubs_.contains(connection->id()));
            egress_hubs_.erase(connection->id());
        }
    default:
        HLOG(FATAL) << "Unknown connection type: " << connection->type();
    }
}

server::EgressHub* Storage::CreateEgressHub(protocol::ConnType conn_type,
                                            uint16_t dst_node_id,
                                            server::IOWorker* io_worker) {
    struct sockaddr_in addr;
    if (!node_watcher_.GetNodeAddr(server::NodeWatcher::GetDstNodeType(conn_type),
                                   dst_node_id, &addr)) {
        return nullptr;
    }
    auto egress_hub = std::make_unique<server::EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        &addr, absl::GetFlag(FLAGS_message_conn_per_worker));
    uint16_t src_node_id = node_id_;
    egress_hub->SetHandshakeMessageCallback(
        [conn_type, src_node_id] (std::string* handshake) {
            *handshake = protocol::EncodeHandshakeMessage(conn_type, src_node_id);
        }
    );
    RegisterConnection(io_worker, egress_hub.get());
    DCHECK_GE(egress_hub->id(), 0);
    server::EgressHub* hub = egress_hub.get();
    {
        absl::MutexLock lk(&conn_mu_);
        DCHECK(!egress_hubs_.contains(egress_hub->id()));
        egress_hubs_[egress_hub->id()] = std::move(egress_hub);
    }
    return hub;
}

#if 0
void RocksDBStorage::Add(uint64_t seqnum, std::span<const char> data) {
    auto status = db_->Put(rocksdb::WriteOptions(),
                           /* key= */   SeqNumHexStr(seqnum),
                           /* value= */ rocksdb::Slice(data.data(), data.size()));
    if (!status.ok()) {
        LOG(FATAL) << "RocksDB put failed: " << status.ToString();
    }
}

bool RocksDBStorage::Read(uint64_t seqnum, std::string* data) {
    auto status = db_->Get(rocksdb::ReadOptions(),
                           /* key= */   SeqNumHexStr(seqnum),
                           /* value= */ data);
    if (!status.ok() && !status.IsNotFound()) {
        LOG(FATAL) << "RocksDB get failed: " << status.ToString();
    }
    return status.ok();
}
#endif

}  // namespace log
}  // namespace faas
