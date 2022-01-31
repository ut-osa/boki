#include "log/storage_base.h"

#include "log/flags.h"
#include "server/constants.h"
#include "utils/fs.h"

#include <sys/eventfd.h>

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
    : ServerBase(fmt::format("storage_{}", node_id),
                 absl::GetFlag(FLAGS_slog_storage_enable_journal)),
      node_id_(node_id),
      db_(nullptr) {}

void StorageBase::StartInternal() {
    SetupZKWatchers();
    SetupTimers();
    log_cache_.emplace(absl::GetFlag(FLAGS_slog_storage_cache_cap_mb));
    if (db_enabled()) {
        SetupDB();
        db_workers_.emplace(this, absl::GetFlag(FLAGS_slog_storage_flusher_threads));
    }
    indexer_.emplace(db_path_, journal_enabled());
}

void StorageBase::StopInternal() {
    if (db_workers_.has_value()) {
        db_workers_->SignalAllThreads();
        db_workers_->JoinAllThreads();
    }
}

void StorageBase::SetupDB() {
    std::string db_backend = absl::GetFlag(FLAGS_slog_storage_backend);
    if (db_backend == "rocksdb") {
        db_.reset(new RocksDBBackend(db_path_));
    } else if (db_backend == "lmdb") {
        db_.reset(new LMDBBackend(db_path_));
    } else if (db_backend == "tkrzw_hash") {
        db_.reset(new TkrzwDBMBackend(TkrzwDBMBackend::kHashDBM, db_path_));
    } else if (db_backend == "tkrzw_tree") {
        db_.reset(new TkrzwDBMBackend(TkrzwDBMBackend::kTreeDBM, db_path_));
    } else if (db_backend == "tkrzw_skip") {
        db_.reset(new TkrzwDBMBackend(TkrzwDBMBackend::kSkipDBM, db_path_));
    } else {
        HLOG(FATAL) << "Unknown storage backend: " << db_backend;
    }
}

void StorageBase::SetupZKWatchers() {
    view_watcher_.SetViewCreatedCallback(
        [this] (const View* view) {
            this->OnViewCreated(view);
            if (db_enabled()) {
                // TODO: This is not always safe, try fix it
                for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                    if (view->is_active_phylog(sequencer_id)) {
                        db_->InstallLogSpace(bits::JoinTwo16(view->id(), sequencer_id));
                    }
                }
            }
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
    CreatePeriodicTimer(
        kLogTrimCollectorTimerId,
        absl::Milliseconds(absl::GetFlag(FLAGS_slog_storage_trim_gc_internval_ms)),
        [this] () { this->CollectLogTrimOps(); }
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
    case SharedLogOpType::SET_AUXDATA:
        OnRecvLogAuxData(message, payload);
        break;
    default:
        UNREACHABLE();
    }
}

std::optional<LogEntryProto> StorageBase::GetLogEntryFromDB(uint64_t seqnum) {
    DCHECK(db_enabled());
    auto data = db_->Get(bits::HighHalf64(seqnum), bits::LowHalf64(seqnum));
    if (!data.has_value()) {
        return std::nullopt;
    }
    LogEntryProto log_entry_proto;
    if (!log_entry_proto.ParseFromString(*data)) {
        HLOG(FATAL) << "Failed to parse LogEntryProto";
    }
    return log_entry_proto;
}

void StorageBase::TrimLogEntries(std::span<const uint64_t> seqnums) {
    DCHECK(db_enabled());
    absl::flat_hash_map</* logspace_id */ uint32_t, std::vector<uint32_t>> grouped;
    for (uint64_t seqnum : seqnums) {
        uint32_t logspace_id = bits::HighHalf64(seqnum);
        grouped[logspace_id].push_back(bits::LowHalf64(seqnum));
    }
    for (const auto& [logspace_id, keys] : grouped) {
        db_->Delete(logspace_id, VECTOR_AS_SPAN(keys));
    }
}

void StorageBase::IndexerInsert(const LogStorage::Entry* log_entry) {
    StorageIndexer::Record indexer_record;
    indexer_record.seqnum = log_entry->metadata.seqnum;
    indexer_record.user_logspace = log_entry->metadata.user_logspace;
    if (journal_enabled()) {
        const JournalRecord& journal_record = std::get<JournalRecord>(log_entry->data);
        indexer_record.journal_file_id = journal_record.file->file_id();
        indexer_record.journal_offset = journal_record.offset;
        journal_record.file->Ref();
    }
    indexer()->Put(indexer_record);
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
    message.payload_size = gsl::narrow_cast<uint32_t>(serialized_data.size());
    for (uint16_t engine_id : sequencer_node->GetIndexEngineNodes()) {
        SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE,
                             engine_id, message, STRING_AS_SPAN(serialized_data));
    }
}

bool StorageBase::SendSequencerMessage(uint16_t sequencer_id,
                                       SharedLogMessage* message,
                                       std::span<const char> payload) {
    message->origin_node_id = node_id_;
    message->payload_size = gsl::narrow_cast<uint32_t>(payload.size());
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_SEQUENCER,
                                sequencer_id, *message, payload);
}

bool StorageBase::SendEngineResponse(const SharedLogMessage& request,
                                     SharedLogMessage* response,
                                     std::span<const char> payload1,
                                     std::span<const char> payload2,
                                     std::span<const char> payload3) {
    response->origin_node_id = node_id_;
    response->hop_times = request.hop_times + 1;
    response->payload_size = gsl::narrow_cast<uint32_t>(
        payload1.size() + payload2.size() + payload3.size());
    response->client_data = request.client_data;
    return SendSharedLogMessage(protocol::ConnType::STORAGE_TO_ENGINE,
                                request.origin_node_id, *response,
                                payload1, payload2, payload3);
}

void StorageBase::OnRecvSharedLogMessage(int conn_type, uint16_t src_node_id,
                                         const SharedLogMessage& message,
                                         std::span<const char> payload) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    DCHECK(
        (conn_type == kSequencerIngressTypeId && op_type == SharedLogOpType::METALOGS)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::READ_AT)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::REPLICATE)
     || (conn_type == kEngineIngressTypeId && op_type == SharedLogOpType::SET_AUXDATA)
    ) << fmt::format("Invalid combination: conn_type={:#x}, op_type={:#x}",
                     conn_type, message.op_type);
    MessageHandler(message, payload);
}

bool StorageBase::SendSharedLogMessage(protocol::ConnType conn_type, uint16_t dst_node_id,
                                       const SharedLogMessage& message,
                                       std::span<const char> payload1,
                                       std::span<const char> payload2,
                                       std::span<const char> payload3) {
    DCHECK_EQ(size_t{message.payload_size}, payload1.size() + payload2.size() + payload3.size());
    EgressHub* hub = CurrentIOWorkerChecked()->PickOrCreateConnection<EgressHub>(
        ServerBase::GetEgressHubTypeId(conn_type, dst_node_id),
        absl::bind_front(&StorageBase::CreateEgressHub, this, conn_type, dst_node_id));
    if (hub == nullptr) {
        return false;
    }
    hub->SendMessage(VAR_AS_CHAR_SPAN(message, SharedLogMessage), payload1, payload2, payload3);
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
    case kEngineIngressTypeId:
        DCHECK(ingress_conns_.contains(connection->id()));
        ingress_conns_.erase(connection->id());
        break;
    case kSequencerEgressHubTypeId:
    case kEngineEgressHubTypeId:
        {
            absl::MutexLock lk(&conn_mu_);
            DCHECK(egress_hubs_.contains(connection->id()));
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

bool StorageBase::FindJournalRecord(uint64_t seqnum, JournalRecord* record) {
    DCHECK(journal_enabled());
    int file_id;
    if (!indexer()->GetJournalLocation(seqnum, &file_id, &record->offset)) {
        return false;
    }
    server::JournalFile* file;
    {
        absl::ReaderMutexLock lk(&journal_file_mu_);
        if (journal_files_.contains(file_id)) {
            file = journal_files_.at(file_id);
        } else {
            return false;
        }
    }
    record->file = file->MakeAccessor();
    return true;
}

void StorageBase::OnJournalFileCreated(server::JournalFile* file) {
    DCHECK(journal_enabled());
    absl::MutexLock lk(&journal_file_mu_);
    DCHECK(!journal_files_.contains(file->file_id()));
    journal_files_[file->file_id()] = file;
}

void StorageBase::OnJournalFileClosed(server::JournalFile* file) {
    DCHECK(journal_enabled());
    DCHECK(file->owner()->WithinMyEventLoopThread());
    {
        absl::MutexLock lk(&journal_file_mu_);
        DCHECK(journal_files_.contains(file->file_id()));
        journal_files_.erase(file->file_id());
    }
    file->Remove();
}

}  // namespace log
}  // namespace faas
