#include "log/storage.h"

#include "log/flags.h"
#include "log/utils.h"
#include "utils/bits.h"
#include "utils/io.h"
#include "utils/timerfd.h"
#include "server/constants.h"
#include "server/io_worker.h"

#define LOG_HEADER log_header_

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

Storage::Storage(uint16_t node_id)
    : StorageBase(node_id),
      log_header_(fmt::format("Storage[{}-N]: ", node_id)),
      current_view_(nullptr),
      view_finalized_(false) {}

void Storage::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_storage_node(my_node_id());
    if (!contains_myself) {
        HLOG_F(WARNING, "View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                if (!view->is_active_phylog(sequencer_id)) {
                    continue;
                }
                storage_collection_.InstallLogSpace(std::make_unique<LogStorage>(
                    my_node_id(), view, sequencer_id));
            }
        }
        future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        view_finalized_ = false;
        log_header_ = fmt::format("Storage[{}-{}]: ", my_node_id(), view->id());
    }
    if (!ready_requests.empty()) {
        HLOG_F(INFO, "{} requests for the new view", ready_requests.size());
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] () {
                ProcessRequests(requests);
            }
        );
    }
}

void Storage::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    LogStorage::ReadResultVec results;
    std::vector<IndexDataProto> index_data_vec;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        storage_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [&, finalized_view] (uint32_t logspace_id,
                                 LockablePtr<LogStorage> storage_ptr) {
                log_utils::FinalizeLogSpace<LogStorage>(storage_ptr, finalized_view);
                auto locked_storage = storage_ptr.Lock();
                LogStorage::ReadResultVec tmp;
                locked_storage->PollReadResults(&tmp);
                results.insert(results.end(), tmp.begin(), tmp.end());
                if (auto index_data = locked_storage->PollIndexData(); index_data.has_value()) {
                    index_data_vec.push_back(std::move(*index_data));
                }
            }
        );
        view_finalized_ = true;
    }
    if (!results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, results = std::move(results)] {
                ProcessReadResults(results);
            }
        );
    }
    if (!index_data_vec.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, view = finalized_view->view(),
                      index_data_vec = std::move(index_data_vec)] {
                for (const IndexDataProto& index_data : index_data_vec) {
                    SendIndexData(view, index_data);
                }
            }
        );
    }
}

#define ONHOLD_IF_FROM_FUTURE_VIEW(MESSAGE_VAR, PAYLOAD_VAR)        \
    do {                                                            \
        if (current_view_ == nullptr                                \
                || (MESSAGE_VAR).view_id > current_view_->id()) {   \
            future_requests_.OnHoldRequest(                         \
                (MESSAGE_VAR).view_id,                              \
                SharedLogRequest(MESSAGE_VAR, PAYLOAD_VAR));        \
            return;                                                 \
        }                                                           \
    } while (0)

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)                       \
    do {                                                            \
        if (current_view_ != nullptr                                \
                && (MESSAGE_VAR).view_id < current_view_->id()) {   \
            HLOG_F(WARNING, "Receive outdate request from view {}", \
                   (MESSAGE_VAR).view_id);                          \
            return;                                                 \
        }                                                           \
    } while (0)

#define RETURN_IF_LOGSPACE_FINALIZED(LOGSPACE_PTR)                  \
    do {                                                            \
        if ((LOGSPACE_PTR)->finalized()) {                          \
            uint32_t logspace_id = (LOGSPACE_PTR)->identifier();    \
            HLOG_F(WARNING, "LogSpace {} is finalized",             \
                   bits::HexStr0x(logspace_id));                    \
            return;                                                 \
        }                                                           \
    } while (0)

#define VALIDATE_LOG_PARTS(METADATA_VAR, USER_TAGS_VAR, DATA_VAR)     \
    do {                                                              \
        auto checksum = log_utils::ComputeLogChecksum(                \
            (METADATA_VAR), (USER_TAGS_VAR), (DATA_VAR));             \
        if ((METADATA_VAR).checksum != checksum) {                    \
            LOG(FATAL) << "Mismatched checksum for log entry data!";  \
        }                                                             \
    } while (0)

#define VALIDATE_LOG_ENTRY(LOG_ENTRY_VAR)                             \
    do {                                                              \
        auto checksum = log_utils::ComputeLogChecksum(LOG_ENTRY_VAR); \
        if ((LOG_ENTRY_VAR).metadata.checksum != checksum) {          \
            LOG(FATAL) << "Mismatched checksum for log entry data!";  \
        }                                                             \
    } while (0)

void Storage::HandleReadAtRequest(const SharedLogMessage& request) {
    DCHECK(SharedLogMessageHelper::GetOpType(request) == SharedLogOpType::READ_AT);
    LockablePtr<LogStorage> storage_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
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
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::REPLICATE);
    LogMetaData metadata = log_utils::GetMetaDataFromMessage(message);
    std::span<const uint64_t> user_tags;
    std::span<const char> log_data;
    log_utils::SplitPayloadForMessage(message, payload, &user_tags, &log_data,
                                      /* aux_data= */ nullptr);
    VALIDATE_LOG_PARTS(metadata, user_tags, log_data);
    LogStorage::Entry entry {
        .recv_timestamp = GetMonotonicMicroTimestamp(),
        .metadata       = std::move(metadata),
        .user_tags      = UserTagVec(user_tags.begin(), user_tags.end()),
        .data           = JournalRecord {},
    };

    const View* view = nullptr;
    LockablePtr<LogStorage> storage_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        view = current_view_;
        storage_ptr = storage_collection_.GetLogSpaceChecked(message.logspace_id);
    }

    auto store_fn = [this] (const View* desired_view,
                            LockablePtr<LogStorage> storage_ptr,
                            LogStorage::Entry entry) {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (current_view_ != desired_view) {
            return;
        }
        auto locked_storage = storage_ptr.Lock();
        RETURN_IF_LOGSPACE_FINALIZED(locked_storage);
        if (!locked_storage->Store(std::move(entry))) {
            HLOG(FATAL) << "Failed to store log entry";
        }
    };

    if (journal_enabled()) {
        log_cache()->PutByLocalId(metadata, user_tags, log_data);
        auto binded_store_fn = absl::bind_front(store_fn, view, storage_ptr);
        CurrentIOWorkerChecked()->JournalAppend(
            kLogEntryJournalRecordType,
            {VAR_AS_CHAR_SPAN(message, SharedLogMessage), payload},
            [binded_store_fn, entry] (server::JournalFile* file, size_t offset) {
                LogStorage::Entry new_entry = std::move(entry);
                new_entry.data = JournalRecord {
                    .file = file->MakeAccessor(),
                    .offset = offset,
                };
                binded_store_fn(std::move(new_entry));
            }
        );
    } else {
        std::string data;
        data.append(reinterpret_cast<const char*>(&message), sizeof(SharedLogMessage));
        data.append(payload.data(), payload.size());
        entry.data = std::move(data);
        store_fn(view, std::move(storage_ptr), std::move(entry));
    }
}

void Storage::OnRecvNewMetaLogs(const SharedLogMessage& message,
                                std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), message.logspace_id);
    const View* view = nullptr;
    LogStorage::ReadResultVec results;
    std::optional<IndexDataProto> index_data;
    LogStorage::LogEntryVec new_log_entires;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        view = current_view_;
        auto storage_ptr = storage_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_storage = storage_ptr.Lock();
            RETURN_IF_LOGSPACE_FINALIZED(locked_storage);
            for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                locked_storage->ProvideMetaLog(metalog_proto);
            }
            locked_storage->PollReadResults(&results);
            index_data = locked_storage->PollIndexData();
            locked_storage->GrabLogEntriesForPersistence(&new_log_entires);
        }
    }
    ProcessReadResults(results);
    if (index_data.has_value()) {
        SendIndexData(DCHECK_NOTNULL(view), *index_data);
    }
    if (!new_log_entires.empty()) {
        db_workers()->SubmitLogEntriesForFlush(VECTOR_AS_SPAN(new_log_entires));
    }
}

void Storage::OnRecvLogAuxData(const protocol::SharedLogMessage& message,
                               std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::SET_AUXDATA);
    uint64_t seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
    log_cache()->PutAuxData(seqnum, payload);
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW
#undef RETURN_IF_LOGSPACE_FINALIZED

void Storage::ProcessReadResults(const LogStorage::ReadResultVec& results) {
    LogEntry log_entry;
    for (const LogStorage::ReadResult& result : results) {
        const SharedLogMessage& request = result.original_request;
        SharedLogMessage response;
        uint64_t seqnum = bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf);
        if (result.status != LogStorage::ReadResult::kFailed) {
            if (auto cached = log_cache()->GetBySeqnum(seqnum); cached.has_value()) {
                log_entry = std::move(cached.value());
                response = SharedLogMessageHelper::NewReadOkResponse(
                    request.user_metalog_progress);
                SendEngineLogResult(request, &response, log_entry);
                continue;
            }
        }
        switch (result.status) {
        case LogStorage::ReadResult::kInlined:
            log_entry = ReadInlinedLogEntry(seqnum, result.localid, result.data);
            response = SharedLogMessageHelper::NewReadOkResponse(request.user_metalog_progress);
            SendEngineLogResult(request, &response, log_entry);
            break;
        case LogStorage::ReadResult::kLookupDB:
            if (journal_for_storage()) {
                ProcessReadFromJournal(request);
            } else {
                ProcessReadFromDB(request);
            }
            break;
        case LogStorage::ReadResult::kFailed:
            HLOG_F(ERROR, "Failed to read log data (seqnum={})",
                   bits::HexStr0x(bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf)));
            response = SharedLogMessageHelper::NewDataLostResponse();
            SendEngineResponse(request, &response);
            break;
        default:
            UNREACHABLE();
        }
    }
}

void Storage::ProcessReadFromJournal(const SharedLogMessage& request) {
    DCHECK(journal_for_storage());
    uint64_t seqnum = bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf);
    JournalRecord record;
    if (!FindJournalRecord(seqnum, &record)) {
        HLOG_F(ERROR, "Failed to found entry (seqnum={}) from journal",
               bits::HexStr0x(seqnum));
        SharedLogMessage response = SharedLogMessageHelper::NewDataLostResponse();
        SendEngineResponse(request, &response);
        return;
    }
    SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse(
        request.user_metalog_progress);
    LogEntry log_entry = log_utils::ReadLogEntryFromJournal(seqnum, record);
    VALIDATE_LOG_ENTRY(log_entry);
    SendEngineLogResult(request, &response, log_entry);
    log_cache()->PutBySeqnum(log_entry);
}

void Storage::ProcessReadFromDB(const SharedLogMessage& request) {
    DCHECK(db_enabled());
    uint64_t seqnum = bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf);
    LogEntryProto log_entry_proto;
    if (auto tmp = GetLogEntryFromDB(seqnum); tmp.has_value()) {
        log_entry_proto = std::move(*tmp);
    } else if (indexer()->CheckRecentlyTrimmed(seqnum)) {
        SharedLogMessage response = SharedLogMessageHelper::NewReadTrimmedResponse(
            request.user_metalog_progress);
        SendEngineResponse(request, &response);
        return;
    } else {
        HLOG_F(ERROR, "Failed to read log data (seqnum={})", bits::HexStr0x(seqnum));
        SharedLogMessage response = SharedLogMessageHelper::NewDataLostResponse();
        SendEngineResponse(request, &response);
        return;
    }
    SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse(
        request.user_metalog_progress);
    LogMetaData metadata;
    std::span<const uint64_t> user_tags;
    std::span<const char> log_data;
    log_utils::SplitLogEntryProto(log_entry_proto, &metadata, &user_tags, &log_data);
    VALIDATE_LOG_PARTS(metadata, user_tags, log_data);
    SendEngineLogResult(request, &response,
                        metadata, VECTOR_AS_CHAR_SPAN(user_tags), log_data);
    log_cache()->PutBySeqnum(metadata, user_tags, log_data);
}

void Storage::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

void Storage::SendEngineLogResult(const protocol::SharedLogMessage& request,
                                  protocol::SharedLogMessage* response,
                                  const LogMetaData& metadata,
                                  std::span<const char> tags_data,
                                  std::span<const char> log_data) {
    log_utils::PopulateMetaDataToMessage(metadata, response);
    uint64_t seqnum = metadata.seqnum;
    std::optional<std::string> cached_aux_data = log_cache()->GetAuxData(seqnum);
    std::span<const char> aux_data;
    if (cached_aux_data.has_value()) {
        size_t full_size = log_data.size() + tags_data.size() + cached_aux_data->size();
        if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
            aux_data = STRING_AS_SPAN(*cached_aux_data);
        } else {
            HLOG_F(WARNING, "Inline buffer of message not large enough "
                            "for auxiliary data of log (seqnum {}): "
                            "log_size={}, num_tags={} aux_data_size={}",
                   bits::HexStr0x(seqnum), log_data.size(),
                   tags_data.size() / sizeof(uint64_t),
                   cached_aux_data->size());
        }
    }
    response->aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
    SendEngineResponse(request, response, tags_data, log_data, aux_data);
}

void Storage::SendEngineLogResult(const protocol::SharedLogMessage& request,
                                  protocol::SharedLogMessage* response,
                                  const LogEntry& log_entry) {
    SendEngineLogResult(request, response,
                        log_entry.metadata,
                        VECTOR_AS_CHAR_SPAN(log_entry.user_tags),
                        STRING_AS_SPAN(log_entry.data));
}

LogEntry Storage::ReadInlinedLogEntry(uint64_t seqnum, uint64_t localid,
                                      const LogStorage::LogData& data) {
    LogEntry log_entry;
    if (journal_enabled()) {
        uint32_t logspace_id = bits::HighHalf64(seqnum);
        if (auto cached = log_cache()->GetByLocalId(logspace_id, localid); cached.has_value()) {
            log_entry = std::move(cached.value());
            log_entry.metadata.seqnum = seqnum;
        } else {
            HVLOG_F(1, "Failed to find log entry (seqnum {}, localid {}) from cache, "
                    "read from journal instead",
                    bits::HexStr0x(seqnum), bits::HexStr0x(localid));
            const JournalRecord& record = std::get<JournalRecord>(data);
            log_entry = log_utils::ReadLogEntryFromJournal(seqnum, record);
        }
    } else {
        const std::string& buf = std::get<std::string>(data);
        DCHECK_GE(buf.size(), sizeof(SharedLogMessage));
        const SharedLogMessage* message = reinterpret_cast<const SharedLogMessage*>(buf.data());
        std::span<const char> payload(
            buf.data() + sizeof(SharedLogMessage),
            buf.size() - sizeof(SharedLogMessage));
        std::span<const uint64_t> user_tags;
        std::span<const char> log_data;
        log_utils::SplitPayloadForMessage(*message, payload, &user_tags, &log_data,
                                          /* aux_data= */ nullptr);
        log_entry.metadata = log_utils::GetMetaDataFromMessage(*message);
        log_entry.metadata.seqnum = seqnum;
        log_entry.user_tags.assign(user_tags.begin(), user_tags.end());
        log_entry.data.assign(log_data.data(), log_data.size());
    }
    DCHECK_EQ(log_entry.metadata.localid, localid);
    VALIDATE_LOG_ENTRY(log_entry);
    log_cache()->PutBySeqnum(log_entry);
    return log_entry;
}

#undef VALIDATE_LOG_PARTS
#undef VALIDATE_LOG_ENTRY

void Storage::SendShardProgressIfNeeded() {
    std::vector<std::pair<uint32_t, std::vector<uint32_t>>> progress_to_send;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (current_view_ == nullptr || view_finalized_) {
            return;
        }
        storage_collection_.ForEachActiveLogSpace(
            current_view_,
            [&progress_to_send] (uint32_t logspace_id,
                                 LockablePtr<LogStorage> storage_ptr) {
                auto locked_storage = storage_ptr.Lock();
                if (!locked_storage->frozen() && !locked_storage->finalized()) {
                    auto progress = locked_storage->GrabShardProgressForSending();
                    if (progress.has_value()) {
                        progress_to_send.emplace_back(logspace_id, std::move(*progress));
                    }
                }
            }
        );
    }
    for (const auto& entry : progress_to_send) {
        uint32_t logspace_id = entry.first;
        SharedLogMessage message = SharedLogMessageHelper::NewShardProgressMessage(logspace_id);
        SendSequencerMessage(bits::LowHalf32(logspace_id), &message,
                             VECTOR_AS_CHAR_SPAN(entry.second));
    }
}

void Storage::CollectLogTrimOps() {
    absl::flat_hash_map</* logspace_id */ uint32_t, LogStorage::TrimOpVec> all_trim_ops;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (current_view_ == nullptr || view_finalized_) {
            return;
        }
        storage_collection_.ForEachActiveLogSpace(
            current_view_,
            [&all_trim_ops] (uint32_t logspace_id, LockablePtr<LogStorage> storage_ptr) {
                auto tmp = storage_ptr.ReaderLock()->FetchTrimOps();
                if (!tmp.empty()) {
                    all_trim_ops[logspace_id] = tmp;
                }
            }
        );
    }

    std::vector<uint64_t> trimmed_seqnums;
    for (const auto& [logspace_id, trim_ops] : all_trim_ops) {
        for (const auto& trim_op : trim_ops) {
            StorageIndexer::SeqnumVec seqnums;
            indexer()->TrimSeqnumsUntil(
                trim_op.user_logspace, trim_op.trim_seqnum, &seqnums);
            HVLOG_F(1, "Found {} seqnums to trim for TrimOp: user_logspace={}, trim_seqnum={}",
                    seqnums.size(), trim_op.user_logspace, bits::HexStr0x(trim_op.trim_seqnum));
            trimmed_seqnums.insert(trimmed_seqnums.end(), seqnums.begin(), seqnums.end());
        }
    }
    indexer()->Flush();

    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        for (const auto& [logspace_id, trim_ops] : all_trim_ops) {
            auto locked_storage = storage_collection_.GetLogSpaceChecked(logspace_id).Lock();
            for (const auto& trim_op : trim_ops) {
                locked_storage->MarkTrimOpFinished(trim_op);
            }
        }
    }

    if (trimmed_seqnums.empty()) {
        return;
    }
    if (db_enabled()) {
        HVLOG_F(1, "Going to trim {} seqnums from DB", trimmed_seqnums.size());
        db_workers()->SubmitSeqnumsForTrim(VECTOR_AS_SPAN(trimmed_seqnums));
    } else {
        HVLOG_F(1, "Trim {} seqnums from journal", trimmed_seqnums.size());
        for (uint64_t seqnum : trimmed_seqnums) {
            JournalRecord record;
            if (FindJournalRecord(seqnum, &record)) {
                record.file->Unref();
            } else {
                HLOG_F(ERROR, "Failed to found entry (seqnum={}) from journal",
                       bits::HexStr0x(seqnum));
            }
        }
    }
}

void Storage::DBFlushLogEntries(std::span<const LogStorage::Entry* const> entries) {
    if (!db_enabled()) {
        // Journal is used for storage, nothing to do here
        return;
    }
    HVLOG_F(1, "Going to flush {} entries to DB", entries.size());
    absl::flat_hash_set<uint32_t> logspace_ids;
    for (const LogStorage::Entry* entry : entries) {
        uint32_t logspace_id = bits::HighHalf64(entry->metadata.seqnum);
        logspace_ids.insert(logspace_id);
    }
    for (uint32_t logspace_id : logspace_ids) {
        DBInterface::Batch batch;
        batch.logspace_id = logspace_id;
        for (const LogStorage::Entry* entry : entries) {
            if (logspace_id != bits::HighHalf64(entry->metadata.seqnum)) {
                continue;
            }
            LogEntry log_entry = ReadInlinedLogEntry(
                entry->metadata.seqnum, entry->metadata.localid, entry->data);
            batch.keys.push_back(bits::LowHalf64(log_entry.metadata.seqnum));
            batch.data.push_back(log_utils::SerializedLogEntryToProto(log_entry));
        }
        log_db()->PutBatch(batch);
    }
}

void Storage::CommitLogEntries(std::span<const LogStorage::Entry* const> entries) {
    HVLOG_F(1, "Will commit the persistence of {} entries", entries.size());

    for (const auto& entry : entries) {
        IndexerInsert(entry);
    }
    indexer()->Flush();

    absl::flat_hash_map<uint32_t, std::vector<uint64_t>> seqnums_by_logspace;
    for (const LogStorage::Entry* entry : entries) {
        uint64_t seqnum = entry->metadata.seqnum;
        uint32_t logspace_id = bits::HighHalf64(seqnum);
        seqnums_by_logspace[logspace_id].push_back(seqnum);
    }

    std::vector<uint32_t> finalized_logspaces;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        for (const auto& [logspace_id, seqnums] : seqnums_by_logspace) {
            auto storage_ptr = storage_collection_.GetLogSpaceChecked(logspace_id);
            {
                auto locked_storage = storage_ptr.Lock();
                locked_storage->LogEntriesPersisted(VECTOR_AS_SPAN(seqnums));
                if (locked_storage->finalized() && locked_storage->all_persisted()) {
                    finalized_logspaces.push_back(locked_storage->identifier());
                }
            }
        }
    }
    if (!finalized_logspaces.empty()) {
        absl::MutexLock view_lk(&view_mu_);
        for (uint32_t logspace_id : finalized_logspaces) {
            if (storage_collection_.FinalizeLogSpace(logspace_id)) {
                HLOG_F(INFO, "Finalize storage log space {}", bits::HexStr0x(logspace_id));
            } else {
                HLOG_F(ERROR, "Storage log space {} not active, cannot finalize",
                       bits::HexStr0x(logspace_id));
            }
        }
    }
}

}  // namespace log
}  // namespace faas
