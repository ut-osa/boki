#include "log/storage.h"

#include "utils/bits.h"

#define log_header_ "Storage: "

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

Storage::Storage(uint16_t node_id)
    : StorageBase(node_id) {}

Storage::~Storage() {}

void Storage::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    std::vector<std::unique_ptr<LogStorage>> new_storages;
    new_storages.reserve(view->num_sequencer_nodes());
    for (uint16_t sequencer_id : view->GetSequencerNodes()) {
        new_storages.push_back(std::make_unique<LogStorage>(
            my_node_id(), view, sequencer_id));
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock core_lk(&core_mu_);
        storage_collection_.OnNewView(view, std::move(new_storages));
        {
            absl::MutexLock future_request_lk(&future_request_mu_);
            future_requests_.OnNewView(view, &ready_requests);
        }
    }
    if (!ready_requests.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] {
                ProcessRequests(requests);
            }
        );
    }
}

void Storage::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    const View* view = finalized_view->view();
    absl::MutexLock core_lk(&core_mu_);
    for (uint16_t sequencer_id : view->GetSequencerNodes()) {
        uint32_t logspace_id = bits::JoinTwo16(view->id(), sequencer_id);
        LockablePtr<LogStorage> storage_ptr = storage_collection_.GetLogSpace(logspace_id);
        auto locked_storage = storage_ptr.Lock();
        bool success = locked_storage->Finalize(
            finalized_view->final_metalog_position(logspace_id),
            finalized_view->tail_metalogs(logspace_id));
        if (!success) {
            HLOG(FATAL) << fmt::format("Failed to finalize log space {}",
                                        bits::HexStr0x(logspace_id));
        }
    }
}

void Storage::HandleReadAtRequest(const SharedLogMessage& request) {
    DCHECK(SharedLogMessageHelper::GetOpType(request) == SharedLogOpType::READ_AT);
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
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::REPLICATE);
    LockablePtr<LogStorage> storage_ptr;
    {
        uint32_t logspace_id = message.logspace_id;
        absl::ReaderMutexLock core_lk(&core_mu_);
        if (storage_collection_.is_from_future_view(logspace_id)) {
            absl::MutexLock future_request_lk(&future_request_mu_);
            future_requests_.OnHoldRequest(SharedLogRequest(message, payload));
            return;
        }
        if (!storage_collection_.is_from_current_view(logspace_id)) {
            HLOG(WARNING) << fmt::format("Receive outdate replicate request from view {}",
                                         bits::HighHalf32(logspace_id));
            return;
        }
        storage_ptr = storage_collection_.GetLogSpace(logspace_id);
        if (storage_ptr == nullptr) {
            HLOG(FATAL) << fmt::format("Failed to find log space {}",
                                       bits::HexStr0x(logspace_id));
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
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto;
    if (!metalogs_proto.ParseFromArray(payload.data(), payload.size())) {
        HLOG(FATAL) << "Failed to parse MetaLogsProto";
    }
    if (metalogs_proto.metalogs_size() == 0) {
        HLOG(FATAL) << "Empty MetaLogsProto";
    }
    uint32_t logspace_id = metalogs_proto.metalogs(0).logspace_id();
    for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
        if (metalog_proto.logspace_id() != logspace_id) {
            HLOG(FATAL) << "Meta logs in on MetaLogsProto must have the same logspace_id";
        }
    }
    LockablePtr<LogStorage> storage_ptr;
    {
        absl::ReaderMutexLock core_lk(&core_mu_);
        if (storage_collection_.is_from_future_view(logspace_id)) {
            absl::MutexLock future_request_lk(&future_request_mu_);
            future_requests_.OnHoldRequest(SharedLogRequest(message, payload));
            return;
        }
        if (!storage_collection_.is_from_current_view(logspace_id)) {
            HLOG(WARNING) << fmt::format("Receive outdate meta logs from view {}",
                                         bits::HighHalf32(logspace_id));
            return;
        }
        storage_ptr = storage_collection_.GetLogSpace(logspace_id);
        if (storage_ptr == nullptr) {
            HLOG(FATAL) << fmt::format("Failed to find log space {}",
                                       bits::HexStr0x(logspace_id));
        }
    }
    LogStorage::ReadResultVec results;
    {
        auto locked_storage = storage_ptr.Lock();
        for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
            locked_storage->ProvideMetaLog(metalog_proto);
        }
        locked_storage->PollReadResults(&results);
    }
    ProcessReadResults(results);
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
    LogEntryProto log_entry;
    bool found = GetLogEntryFromDB(request.logspace_id, request.seqnum, &log_entry);
    if (!found) {
        HLOG(ERROR) << fmt::format("Failed to read log data (logspace={}, seqnum={})",
                                   bits::HexStr0x(request.logspace_id),
                                   bits::HexStr0x(request.seqnum));
        SharedLogMessage response = SharedLogMessageHelper::NewDataLostResponse();
        SendEngineResponse(request, &response);
        return;
    }
    SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse();
    PopulateMetaDataToResponse(log_entry, &response);
    DCHECK_EQ(response.logspace_id, request.logspace_id);
    DCHECK_EQ(response.seqnum, request.seqnum);
    response.metalog_position = request.metalog_position;
    SendEngineResponse(request, &response, STRING_TO_SPAN(log_entry.data()));
}

void Storage::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_TO_SPAN(request.payload));
    }
}

void Storage::BackgroundThreadMain() {
    bool running = true;
    while (running) {

        running = state_.load(std::memory_order_acquire) != kStopping;
    }
}

}  // namespace log
}  // namespace faas
