#include "log/storage_base.h"

#include "log/flags.h"
#include "server/constants.h"
#include "utils/fs.h"

#include <rocksdb/db.h>
#include <tkrzw_dbm.h>
#include <tkrzw_dbm_hash.h>

ABSL_FLAG(int, rocksdb_max_background_jobs, 2, "");
ABSL_FLAG(bool, rocksdb_enable_compression, false, "");

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
      db_(nullptr),
      background_thread_("BG", [this] { this->BackgroundThreadMain(); }) {
    std::string db_backend = absl::GetFlag(FLAGS_slog_storage_backend);
    if (db_backend == "rocksdb") {
        db_backend_ = DBBackend::kRocksDB;
    } else if (db_backend == "tkrzw_hashdbm") {
        db_backend_ = DBBackend::kTkrzwHashDBM;
    } else {
        LOG(FATAL) << "Unknown storage backend: " << db_backend;
    }
}

StorageBase::~StorageBase() {
    if (db_ != nullptr) {
        switch (db_backend_) {
        case DBBackend::kRocksDB:
            CloseRocksDB();
            break;
        case DBBackend::kTkrzwHashDBM:
            CloseTkrzwDBM();
            break;
        default:
            UNREACHABLE();
        }
    }
}

void StorageBase::StartInternal() {
    SetupDB();
    SetupZKWatchers();
    SetupTimers();
    background_thread_.Start();
}

void StorageBase::StopInternal() {
    background_thread_.Join();
}

void StorageBase::SetupDB() {
    switch (db_backend_) {
    case DBBackend::kRocksDB:
        OpenRocksDB();
        break;
    case DBBackend::kTkrzwHashDBM:
        OpenTkrzwDBM();
        break;
    default:
        UNREACHABLE();
    }
}

void StorageBase::SetupZKWatchers() {
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

void StorageBase::OpenRocksDB() {
    DCHECK(db_backend_ == DBBackend::kRocksDB);
    rocksdb::Options options;
    options.create_if_missing = true;
    options.max_background_jobs = absl::GetFlag(FLAGS_rocksdb_max_background_jobs);
    if (absl::GetFlag(FLAGS_rocksdb_enable_compression)) {
        options.compression = rocksdb::kZSTD;
    } else {
        options.compression = rocksdb::kNoCompression;
    }
    rocksdb::DB* db;
    HLOG(INFO) << fmt::format("Open RocksDB at path {}", db_path_);
    auto status = rocksdb::DB::Open(options, db_path_, &db);
    if (!status.ok()) {
        HLOG(FATAL) << "RocksDB open failed: " << status.ToString();
    }
    db_ = db;
}

void StorageBase::CloseRocksDB() {
    DCHECK(db_backend_ == DBBackend::kRocksDB);
    rocksdb::DB* db = reinterpret_cast<rocksdb::DB*>(DCHECK_NOTNULL(db_));
    db_ = nullptr;
    delete db;
}

void StorageBase::OpenTkrzwDBM() {
    DCHECK(db_backend_ == DBBackend::kTkrzwHashDBM);
    tkrzw::DBM* db = new tkrzw::HashDBM();
    std::string file_path = fs_utils::JoinPath(db_path_, "DB");
    HLOG(INFO) << fmt::format("Open TkrzwHashDBM at path {}", file_path);
    auto status = db->Open(file_path, /* writable= */ true);
    if (!status.IsOK()) {
        HLOG(FATAL) << "Tkrzw open failed: " << tkrzw::ToString(status);
    }
    db_ = db;
}

void StorageBase::CloseTkrzwDBM() {
    DCHECK(db_backend_ == DBBackend::kTkrzwHashDBM);
    tkrzw::DBM* db = reinterpret_cast<tkrzw::DBM*>(DCHECK_NOTNULL(db_));
    db_ = nullptr;
    auto status = db->Close();
    if (!status.IsOK()) {
        HLOG(FATAL) << "Tkrzw close failed: " << tkrzw::ToString(status);
    }
    delete db;
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
static inline std::string GetDBKey(uint64_t seqnum) {
    return bits::HexStr(seqnum);
}

static inline std::string SerializedLogEntry(const LogEntry* log_entry) {
    LogEntryProto log_entry_proto;
    log_entry_proto.set_user_logspace(log_entry->metadata.user_logspace);
    log_entry_proto.set_user_tag(log_entry->metadata.user_tag);
    log_entry_proto.set_seqnum(log_entry->metadata.seqnum);
    log_entry_proto.set_localid(log_entry->metadata.localid);
    log_entry_proto.set_data(log_entry->data);
    std::string data;
    CHECK(log_entry_proto.SerializeToString(&data));
    return data;
}

static inline bool GetRocksDB(rocksdb::DB* db, std::string_view key, std::string* value) {
    auto status = db->Get(rocksdb::ReadOptions(), key, value);
    if (status.IsNotFound()) {
        return false;
    }
    if (!status.ok()) {
        HLOG(FATAL) << "RocksDB Get() failed: " << status.ToString();
    }
    return true;
}

static inline bool GetTkrzwDBM(tkrzw::DBM* db, std::string_view key, std::string* value) {
    auto status = db->Get(key, value);
    return status.IsOK();
}

static inline void PutRocksDB(rocksdb::DB* db,
                              const std::vector<const LogEntry*>& log_entires) {
    rocksdb::WriteBatch batch;
    for (const LogEntry* log_entry : log_entires) {
        std::string db_key = GetDBKey(log_entry->metadata.seqnum);
        std::string db_value = SerializedLogEntry(log_entry);
        batch.Put(db_key, db_value);
    }
    auto status = db->Write(rocksdb::WriteOptions(), &batch);
    if (!status.ok()) {
        LOG(FATAL) << "RocksDB write failed: " << status.ToString();
    }
}

static inline void PutTkrzwDBM(tkrzw::DBM* db,
                               const std::vector<const LogEntry*>& log_entires) {
    for (const LogEntry* log_entry : log_entires) {
        std::string db_key = GetDBKey(log_entry->metadata.seqnum);
        std::string db_value = SerializedLogEntry(log_entry);
        auto status = db->Set(db_key, db_value);
        if (!status.IsOK()) {
            LOG(FATAL) << "Tkrzw set failed: " << tkrzw::ToString(status);
        }
    }
}
}  // namespace

bool StorageBase::GetLogEntryFromDB(uint64_t seqnum, LogEntryProto* log_entry_proto) {
    std::string db_key = GetDBKey(seqnum);
    std::string data;
    bool found = false;
    switch (db_backend_) {
    case DBBackend::kRocksDB:
        found = GetRocksDB(reinterpret_cast<rocksdb::DB*>(db_), db_key, &data);
        break;
    case DBBackend::kTkrzwHashDBM:
        found = GetTkrzwDBM(reinterpret_cast<tkrzw::DBM*>(db_), db_key, &data);
        break;
    default:
        UNREACHABLE();
    }
    if (!found) {
        return false;
    }
    if (!log_entry_proto->ParseFromString(data)) {
        HLOG(FATAL) << "Failed to parse LogEntryProto";
    }
    return true;
}

void StorageBase::PutLogEntriesToDB(const std::vector<const LogEntry*>& log_entires) {
    switch (db_backend_) {
    case DBBackend::kRocksDB:
        PutRocksDB(reinterpret_cast<rocksdb::DB*>(db_), log_entires);
        break;
    case DBBackend::kTkrzwHashDBM:
        PutTkrzwDBM(reinterpret_cast<tkrzw::DBM*>(db_), log_entires);
        break;
    default:
        UNREACHABLE();
    }
}

void StorageBase::SendIndexData(const View* view,
                                const IndexDataProto& index_data_proto) {
    uint32_t logspace_id = index_data_proto.logspace_id();
    DCHECK_EQ(view->id(), bits::HighHalf32(logspace_id));
    const View::Sequencer* sequencer_node = view->GetSequencerNode(
        bits::LowHalf32(logspace_id));
    std::string serialized_data;
    CHECK(index_data_proto.SerializeToString(&serialized_data));
    SharedLogMessage message = SharedLogMessageHelper::NewIndexDataMessage(
        logspace_id);
    message.origin_node_id = node_id_;
    message.payload_size = serialized_data.size();
    for (uint16_t engine_id : sequencer_node->GetIndexEngineNodes()) {
        SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE,
                             engine_id, message, STRING_AS_SPAN(serialized_data));
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

    switch (type) {
    case protocol::ConnType::ENGINE_TO_STORAGE:
        break;
    case protocol::ConnType::SEQUENCER_TO_STORAGE:
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
                             conn_type_id & kConnectionTypeMask, src_node_id)));
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
        break;
    default:
        HLOG(FATAL) << "Unknown connection type: " << connection->type();
    }
}

EgressHub* StorageBase::CreateEgressHub(protocol::ConnType conn_type,
                                        uint16_t dst_node_id,
                                        IOWorker* io_worker) {
    struct sockaddr_in addr;
    if (!node_watcher()->GetNodeAddr(NodeWatcher::GetDstNodeType(conn_type),
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
