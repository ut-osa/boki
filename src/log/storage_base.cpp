#include "log/storage_base.h"

#include "log/flags.h"
#include "server/constants.h"

#define log_header_ "StorageBase: "

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

using server::IOWorker;
using server::ConnectionBase;
using server::IngressConnection;
using server::EgressHub;
using server::NodeWatcher;

StorageBase::StorageBase(uint16_t node_id)
    : ServerBase(fmt::format("storage_{}", node_id)),
      node_id_(node_id),
      background_thread_("Background", [this] { this->BackgroundThreadMain(); }) {}

StorageBase::~StorageBase() {}

void StorageBase::StartInternal() {
    SetupRocksDB();
    SetupZKWatchers();
    SetupTimers();
    background_thread_.Start();
}

void StorageBase::StopInternal() {
    background_thread_.Join();
}

void StorageBase::SetupRocksDB() {
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

void StorageBase::SetupZKWatchers() {
    node_watcher_.StartWatching(zk_session());
    view_watcher_.SetViewCreatedCallback(
        [this] (const View* view) {
            this->OnViewCreated(view);
        }
    );
    view_watcher_.SetViewFinalizedCallback(
        [this] (const FinalizedView* finalized_view) {
            this->OnViewFinalized(finalized_view);
        }
    );
    view_watcher_.StartWatching(zk_session());
}

void StorageBase::SetupTimers() {
    CreatePeriodicTimer(
        kSendShardProgressTimerId,
        absl::Microseconds(absl::GetFlag(FLAGS_slog_local_cut_interval_us)),
        [this] () { this->SendShardProgressIfNeeded(); }
    );
}

void StorageBase::MessageHandler(const SharedLogMessage& message,
                                 std::span<const char> payload) {
    switch (SharedLogMessageHelper::GetOpType(message)) {
    case SharedLogOpType::READ_AT:
        HandleReadAtRequest(message);
        break;
    case SharedLogOpType::REPLICATE:
        HandleReplicateRequest(message, payload);
        break;
    case SharedLogOpType::METALOGS:
        OnRecvNewMetaLogs(message, payload);
        break;
    default:
        UNREACHABLE();
    }
}

namespace {
static inline std::string GetDBKey(uint32_t logspace_id, uint32_t seqnum) {
    return fmt::format("{0:08x}{1:08x}", logspace_id, seqnum);
}
}  // namespace

bool StorageBase::GetLogEntryFromDB(uint32_t logspace_id, uint32_t seqnum,
                                    LogEntryProto* log_entry_proto) {
    std::string db_key = GetDBKey(logspace_id, seqnum);
    std::string data;
    auto status = db_->Get(rocksdb::ReadOptions(), db_key, &data);
    if (status.IsNotFound()) {
        return false;
    }
    if (!status.ok()) {
        HLOG(FATAL) << "RocksDB Get() failed: " << status.ToString();
    }
    if (!log_entry_proto->ParseFromString(data)) {
        HLOG(FATAL) << "Failed to parse LogEntryProto";
    }
    return true;
}

void StorageBase::PutLogEntryToDB(const LogEntry& log_entry) {
    std::string db_key = GetDBKey(log_entry.metadata.logspace_id,
                                  log_entry.metadata.seqnum);
    LogEntryProto log_entry_proto;
    log_entry_proto.set_logspace_id(log_entry.metadata.logspace_id);
    log_entry_proto.set_user_logspace(log_entry.metadata.user_logspace);
    log_entry_proto.set_user_tag(log_entry.metadata.user_tag);
    log_entry_proto.set_seqnum(log_entry.metadata.seqnum);
    log_entry_proto.set_localid(log_entry.metadata.localid);
    log_entry_proto.set_data(log_entry.data);
    std::string data;
    log_entry_proto.SerializeToString(&data);
    auto status = db_->Put(rocksdb::WriteOptions(), db_key, data);
    if (!status.ok()) {
        LOG(FATAL) << "RocksDB put failed: " << status.ToString();
    }
}

bool StorageBase::SendSequencerMessage(uint16_t sequencer_id,
                                       SharedLogMessage* message,
                                       std::span<const char> payload) {
    message->origin_node_id = node_id_;
    message->payload_size = payload.size();
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_SEQUENCER,
                                sequencer_id, *message, payload);
}

bool StorageBase::SendEngineResponse(const SharedLogMessage& request,
                                     SharedLogMessage* response,
                                     std::span<const char> payload) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = payload.size();
    response->client_data = request.client_data;
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE,
                                request.origin_node_id, *response, payload);
}

void StorageBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                         const SharedLogMessage& message,
                                         std::span<const char> payload) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    DCHECK(
        (conn_type == kSequencerIngressTypeId && op_type == SharedLogOpType::METALOGS)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_AT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::REPLICATE)
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

bool StorageBase::SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                                       const SharedLogMessage& message,
                                       std::span<const char> payload) {
    EgressHub* hub = CurrentIOWorkerChecked()->PickOrCreateConnection<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        absl::bind_front(&StorageBase::CreateEgressHub, this, conn_type, dst_node_id));
    if (hub == nullptr) {
        return false;
    }
    std::span<const char> data(reinterpret_cast<const char*>(&message),
                               sizeof(SharedLogMessage));
    hub->SendMessage(data, payload);
    return true;
}

void StorageBase::OnRemoteMessageConn(const protocol::HandshakeMessage& handshake,
                                      int sockfd) {
    protocol::ConnType type = static_cast<protocol::ConnType>(handshake.conn_type);
    uint16_t src_node_id = handshake.src_node_id;

    switch (NodeWatcher::GetSrcNodeType(type)) {
    case NodeWatcher::kEngineNode:
        break;
    case NodeWatcher::kSequencerNode:
        break;
    default:
        HLOG(ERROR) << "Invalid connection type: " << handshake.conn_type;
        close(sockfd);
        return;
    }

    int conn_type_id = ServerBase::GetIngressConnTypeId(type, src_node_id);
    auto connection = std::make_unique<IngressConnection>(
        conn_type_id, sockfd, sizeof(SharedLogMessage));
    connection->SetMessageFullSizeCallback(
        &IngressConnection::SharedLogMessageFullSizeCallback);
    connection->SetNewMessageCallback(
        IngressConnection::BuildNewSharedLogMessageCallback(
            absl::bind_front(&StorageBase::OnRecvSharedLogMessage, this,
                             conn_type_id & kConnectionTypeMask,
                             src_node_id)));
    RegisterConnection(PickIOWorkerForConnType(conn_type_id), connection.get());
    DCHECK_GE(connection->id(), 0);
    DCHECK(!ingress_conns_.contains(connection->id()));
    ingress_conns_[connection->id()] = std::move(connection);
}

void StorageBase::OnConnectionClose(ConnectionBase* connection) {
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

EgressHub* StorageBase::CreateEgressHub(protocol::ConnType conn_type,
                                        uint16_t dst_node_id,
                                        IOWorker* io_worker) {
    struct sockaddr_in addr;
    if (!node_watcher_.GetNodeAddr(NodeWatcher::GetDstNodeType(conn_type),
                                   dst_node_id, &addr)) {
        return nullptr;
    }
    auto egress_hub = std::make_unique<EgressHub>(
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
    EgressHub* hub = egress_hub.get();
    {
        absl::MutexLock lk(&conn_mu_);
        DCHECK(!egress_hubs_.contains(egress_hub->id()));
        egress_hubs_[egress_hub->id()] = std::move(egress_hub);
    }
    return hub;
}

}  // namespace log
}  // namespace faas
