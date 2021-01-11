#include "log/storage.h"

#include "common/flags.h"
#include "server/constants.h"
#include "utils/bits.h"

#define log_header_ "Storage: "

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

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

void Storage::HandleReadAtRequest(const SharedLogMessage& request) {
    LockablePtr<LogStorage> storage_ptr;
    {
        absl::ReaderMutexLock core_lk(&core_mu_);
        if (storage_collection_.is_from_future_view(request.logspace_id)) {
            absl::MutexLock future_request_lk(&future_request_mu_);
            future_requests_.OnHoldRequest(SharedLogRequest(request));
            return;
        }
        storage_ptr = storage_collection_.GetLogSpace(request.logspace_id);
    }
    if (storage_ptr == nullptr) {
        ProcessReadFromDB(request);
        return;
    }
    LogStorage::ReadResultVec results;
    {
        auto locked_storage = storage_ptr.Lock();
        locked_storage->ReadAt(request);
        locked_storage->PollReadResults(&results);
    }
    ProcessReadResults(results);
}

void Storage::HandleReplicateRequest(const SharedLogMessage& message,
                                     std::span<const char> payload) {
    LockablePtr<LogStorage> storage_ptr;
    {
        uint32_t logspace_id = message.logspace_id;
        absl::ReaderMutexLock core_lk(&core_mu_);
        if (storage_collection_.is_from_future_view(logspace_id)) {
            absl::MutexLock future_request_lk(&future_request_mu_);
            future_requests_.OnHoldRequest(SharedLogRequest(message, payload));
            return;
        }
        if (!storage_collection_.is_from_future_view(logspace_id)) {
            HLOG(WARNING) << fmt::format("Receive outdate replicate request from view {}",
                                         bits::HighHalf32(logspace_id));
            return;
        }
        storage_ptr = storage_collection_.GetLogSpace(logspace_id);
        if (storage_ptr == nullptr) {
            HLOG(ERROR) << fmt::format("Failed to find log space {}",
                                       bits::HexStr0x(logspace_id));
            return;
        }
    }
    LogMetaData metadata;
    PopulateMetaDataFromRequest(message, &metadata);
    {
        auto locked_storage = storage_ptr.Lock();
        if (!locked_storage->Store(metadata, payload)) {
            HLOG(ERROR) << "Failed to store log entry";
        }
    }
}

void Storage::OnRecvNewMetaLogs(const SharedLogMessage& message,
                                std::span<const char> payload) {

}

void Storage::MessageHandler(const SharedLogMessage& message,
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

void Storage::ProcessReadResults(const LogStorage::ReadResultVec& results) {
    for (const LogStorage::ReadResult& result : results) {
        const SharedLogMessage& request = result.original_request;
        SharedLogMessage response;
        switch (result.status) {
        case LogStorage::ReadResult::kOK:
            response = SharedLogMessageHelper::NewReadOkResponse();
            PopulateMetaDataToResponse(result.log_entry->metadata, &response);
            DCHECK_EQ(response.logspace_id, request.logspace_id);
            DCHECK_EQ(response.seqnum, request.seqnum);
            response.metalog_position = request.metalog_position;
            SendEngineResponse(request, &response,
                               STRING_TO_SPAN(result.log_entry->data));
            break;
        case LogStorage::ReadResult::kLookupDB:
            ProcessReadFromDB(request);
            break;
        case LogStorage::ReadResult::kFailed:
            HLOG(ERROR) << fmt::format("Failed to read log data (logspace={}, seqnum={})",
                                       bits::HexStr0x(request.logspace_id),
                                       bits::HexStr0x(request.seqnum));
            response = SharedLogMessageHelper::NewDataLostResponse();
            SendEngineResponse(request, &response);
            break;
        default:
            UNREACHABLE();
        }
    }
}

void Storage::ProcessReadFromDB(const SharedLogMessage& request) {
    std::string db_key = GetDBKey(request.logspace_id, request.seqnum);
    std::string data;
    auto status = db_->Get(rocksdb::ReadOptions(), db_key, &data);
    if (status.IsNotFound()) {
        HLOG(ERROR) << fmt::format("Failed to read log data (logspace={}, seqnum={})",
                                   bits::HexStr0x(request.logspace_id),
                                   bits::HexStr0x(request.seqnum));
        SharedLogMessage response = SharedLogMessageHelper::NewDataLostResponse();
        SendEngineResponse(request, &response);
        return;
    }
    if (!status.ok()) {
        HLOG(FATAL) << "RocksDB Get() failed: " << status.ToString();
    }
    LogEntryProto log_entry;
    if (!log_entry.ParseFromString(data)) {
        HLOG(FATAL) << "Failed to parse LogEntryProto";
    }
    SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse();
    PopulateMetaDataToResponse(log_entry, &response);
    DCHECK_EQ(response.logspace_id, request.logspace_id);
    DCHECK_EQ(response.seqnum, request.seqnum);
    response.metalog_position = request.metalog_position;
    SendEngineResponse(request, &response, STRING_TO_SPAN(log_entry.data()));
}

bool Storage::SendSequencerMessage(uint16_t sequencer_id,
                                   SharedLogMessage* message,
                                   std::span<const char> payload) {
    message->origin_node_id = node_id_;
    message->payload_size = payload.size();
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_SEQUENCER,
                                sequencer_id, *message, payload);
}

bool Storage::SendEngineResponse(const SharedLogMessage& request,
                                 SharedLogMessage* response,
                                 std::span<const char> payload) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = payload.size();
    response->client_data = request.client_data;
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE,
                                request.origin_node_id, *response, payload);
}

void Storage::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
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
                             conn_type_id & kConnectionTypeMask,
                             src_node_id)));
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
#endif

}  // namespace log
}  // namespace faas
