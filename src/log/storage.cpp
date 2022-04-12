#include "log/storage.h"

#include "log/flags.h"
#include "log/utils.h"
#include "utils/bits.h"
#include "utils/io.h"
#include "utils/timerfd.h"

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

Storage::~Storage() {}

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
                log_utils::FinalizedLogSpace<LogStorage>(storage_ptr, finalized_view);
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
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto storage_ptr = storage_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_storage = storage_ptr.Lock();
            RETURN_IF_LOGSPACE_FINALIZED(locked_storage);
            if (!locked_storage->Store(metadata, user_tags, log_data)) {
                HLOG(ERROR) << "Failed to store log entry";
            }
        }
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
        }
    }
    ProcessReadResults(results);
    if (index_data.has_value()) {
        SendIndexData(DCHECK_NOTNULL(view), *index_data);
    }
}

void Storage::OnRecvLogAuxData(const protocol::SharedLogMessage& message,
                               std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::SET_AUXDATA);
    uint64_t seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
    LogCachePutAuxData(seqnum, payload);
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW
#undef RETURN_IF_LOGSPACE_FINALIZED

void Storage::ProcessReadResults(const LogStorage::ReadResultVec& results) {
    for (const LogStorage::ReadResult& result : results) {
        const SharedLogMessage& request = result.original_request;
        SharedLogMessage response;
        switch (result.status) {
        case LogStorage::ReadResult::kOK:
            response = SharedLogMessageHelper::NewReadOkResponse();
            log_utils::PopulateMetaDataToMessage(result.log_entry->metadata, &response);
            DCHECK_EQ(response.logspace_id, request.logspace_id);
            DCHECK_EQ(response.seqnum_lowhalf, request.seqnum_lowhalf);
            response.user_metalog_progress = request.user_metalog_progress;
            SendEngineLogResult(request, &response,
                                VECTOR_AS_CHAR_SPAN(result.log_entry->user_tags),
                                STRING_AS_SPAN(result.log_entry->data));
            break;
        case LogStorage::ReadResult::kLookupDB:
            ProcessReadFromDB(request);
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

void Storage::ProcessReadFromDB(const SharedLogMessage& request) {
    uint64_t seqnum = bits::JoinTwo32(request.logspace_id, request.seqnum_lowhalf);
    LogEntryProto log_entry;
    if (auto tmp = GetLogEntryFromDB(seqnum); tmp.has_value()) {
        log_entry = std::move(*tmp);
    } else {
        HLOG_F(ERROR, "Failed to read log data (seqnum={})", bits::HexStr0x(seqnum));
        SharedLogMessage response = SharedLogMessageHelper::NewDataLostResponse();
        SendEngineResponse(request, &response);
        return;
    }
    SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse();
    log_utils::PopulateMetaDataToMessage(log_entry, &response);
    DCHECK_EQ(response.logspace_id, request.logspace_id);
    DCHECK_EQ(response.seqnum_lowhalf, request.seqnum_lowhalf);
    response.user_metalog_progress = request.user_metalog_progress;
    std::span<const char> user_tags_data(
        reinterpret_cast<const char*>(log_entry.user_tags().data()),
        static_cast<size_t>(log_entry.user_tags().size()) * sizeof(uint64_t));
    SendEngineLogResult(request, &response, user_tags_data,
                        STRING_AS_SPAN(log_entry.data()));
}

void Storage::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

void Storage::SendEngineLogResult(const protocol::SharedLogMessage& request,
                                  protocol::SharedLogMessage* response,
                                  std::span<const char> tags_data,
                                  std::span<const char> log_data) {
    uint64_t seqnum = bits::JoinTwo32(response->logspace_id, response->seqnum_lowhalf);
    std::optional<std::string> cached_aux_data = LogCacheGetAuxData(seqnum);
    std::span<const char> aux_data;
    if (cached_aux_data.has_value()) {
/*
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
*/
        aux_data = STRING_AS_SPAN(*cached_aux_data);
    }
    response->aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
    SendEngineResponse(request, response, tags_data, log_data, aux_data);
}

void Storage::BackgroundThreadMain() {
    int timerfd = io_utils::CreateTimerFd();
    CHECK(timerfd != -1) << "Failed to create timerfd";
    io_utils::FdUnsetNonblocking(timerfd);
    absl::Duration interval = absl::Milliseconds(
        absl::GetFlag(FLAGS_slog_storage_bgthread_interval_ms));
    CHECK(io_utils::SetupTimerFdPeriodic(timerfd, absl::Milliseconds(100), interval))
        << "Failed to setup timerfd with interval " << interval;
    bool running = true;
    while (running) {
        uint64_t exp;
        ssize_t nread = read(timerfd, &exp, sizeof(uint64_t));
        if (nread < 0) {
            PLOG(FATAL) << "Failed to read on timerfd";
        }
        CHECK_EQ(gsl::narrow_cast<size_t>(nread), sizeof(uint64_t));
        FlushLogEntries();
        // TODO: cleanup outdated LogSpace
        running = state_.load(std::memory_order_acquire) != kStopping;
    }
}

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

void Storage::FlushLogEntries() {
    std::vector<std::shared_ptr<const LogEntry>> log_entires;
    std::vector<std::pair<LockablePtr<LogStorage>, uint64_t>> storages;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        storage_collection_.ForEachActiveLogSpace(
            [&log_entires, &storages] (uint32_t logspace_id,
                                       LockablePtr<LogStorage> storage_ptr) {
                auto locked_storage = storage_ptr.ReaderLock();
                std::vector<std::shared_ptr<const LogEntry>> tmp;
                uint64_t new_position;
                if (locked_storage->GrabLogEntriesForPersistence(&tmp, &new_position)) {
                    storages.emplace_back(storage_ptr, new_position);
                    log_entires.insert(log_entires.end(), tmp.begin(), tmp.end());
                }
            }
        );
    }

    if (log_entires.empty()) {
        return;
    }
    HVLOG_F(1, "Will flush {} log entries", log_entires.size());
    for (size_t i = 0; i < log_entires.size(); i++) {
        PutLogEntryToDB(*log_entires[i]);
    }

    std::vector<uint32_t> finalized_logspaces;
    for (auto& [storage_ptr, new_position] : storages) {
        auto locked_storage = storage_ptr.Lock();
        locked_storage->LogEntriesPersisted(new_position);
        if (locked_storage->finalized()
                && new_position >= locked_storage->seqnum_position()) {
            finalized_logspaces.push_back(locked_storage->identifier());
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
