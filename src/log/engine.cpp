#include "log/engine.h"

#include "engine/engine.h"
#include "log/flags.h"
#include "utils/bits.h"
#include "utils/random.h"

namespace faas {
namespace log {

using protocol::Message;
using protocol::MessageHelper;
using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;
using protocol::SharedLogResultType;

Engine::Engine(engine::Engine* engine)
    : EngineBase(engine),
      log_header_(fmt::format("LogEngine[{}-N]: ", my_node_id())),
      current_view_(nullptr),
      current_view_active_(false) {}

Engine::~Engine() {}

void Engine::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_engine_node(my_node_id());
    if (!contains_myself) {
        HLOG_F(WARNING, "View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            const View::Engine* engine_node = view->GetEngineNode(my_node_id());
            for (uint16_t sequencer_id : view->GetSequencerNodes()) {
                if (!view->is_active_phylog(sequencer_id)) {
                    continue;
                }
                producer_collection_.InstallLogSpace(std::make_unique<LogProducer>(
                    my_node_id(), view, sequencer_id));
                if (engine_node->HasIndexFor(sequencer_id)) {
                    index_collection_.InstallLogSpace(std::make_unique<Index>(
                        view, sequencer_id));
                }
            }
        }
        future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        if (contains_myself) {
            current_view_active_ = true;
        }
        views_.push_back(view);
        log_header_ = fmt::format("LogEngine[{}-{}]: ", my_node_id(), view->id());
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

void Engine::OnViewFrozen(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} frozen", view->id());
    absl::MutexLock view_lk(&view_mu_);
    DCHECK_EQ(view->id(), current_view_->id());
    if (view->contains_engine_node(my_node_id())) {
        DCHECK(current_view_active_);
        current_view_active_ = false;
    }
}

void Engine::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
        producer_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &append_results] (uint32_t logspace_id,
                                               LockablePtr<LogProducer> producer_ptr) {
                log_utils::FinalizedLogSpace<LogProducer>(
                    producer_ptr, finalized_view);
                auto locked_producer = producer_ptr.Lock();
                LogProducer::AppendResultVec tmp;
                locked_producer->PollAppendResults(&tmp);
                append_results.insert(append_results.end(), tmp.begin(), tmp.end());
            }
        );
        index_collection_.ForEachActiveLogSpace(
            finalized_view->view(),
            [finalized_view, &query_results] (uint32_t logspace_id,
                                              LockablePtr<Index> index_ptr) {
                log_utils::FinalizedLogSpace<Index>(
                    index_ptr, finalized_view);
                auto locked_index = index_ptr.Lock();
                locked_index->PollQueryResults(&query_results);
            }
        );
    }
    if (!append_results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, results = std::move(append_results)] {
                ProcessAppendResults(results);
            }
        );
    }
    if (!query_results.empty()) {
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, results = std::move(query_results)] {
                ProcessIndexQueryResults(results);
            }
        );
    }
}

namespace {

static void SerializeLogEntry(Message* message, utils::AppendableBuffer* aux_buffer,
                              uint64_t seqnum, std::span<const uint64_t> user_tags,
                              std::span<const char> log_data,
                              std::span<const char> aux_data) {
    DCHECK(aux_buffer->empty());
    *message = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::READ_OK, seqnum);
    message->log_num_tags = gsl::narrow_cast<uint16_t>(user_tags.size());
    message->log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
    size_t full_size = user_tags.size() * sizeof(uint64_t) + log_data.size() + aux_data.size();
    if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
        MessageHelper::AppendInlineData(message, user_tags);
        MessageHelper::AppendInlineData(message, log_data);
        MessageHelper::AppendInlineData(message, aux_data);
    } else {
        aux_buffer->AppendData(VECTOR_AS_CHAR_SPAN(user_tags));
        aux_buffer->AppendData(log_data);
        aux_buffer->AppendData(aux_data);
    }
}

/*
static Message BuildLocalReadOKResponse(uint64_t seqnum,
                                        std::span<const uint64_t> user_tags,
                                        std::span<const char> log_data) {
    Message response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::READ_OK, seqnum);
    if (user_tags.size() * sizeof(uint64_t) + log_data.size() > MESSAGE_INLINE_DATA_SIZE) {
        LOG_F(FATAL, "Log data too large: num_tags={}, size={}",
              user_tags.size(), log_data.size());
    }
    response.log_num_tags = gsl::narrow_cast<uint16_t>(user_tags.size());
    MessageHelper::AppendInlineData(&response, user_tags);
    MessageHelper::AppendInlineData(&response, log_data);
    return response;
}

static Message BuildLocalReadOKResponse(const LogEntry& log_entry) {
    return BuildLocalReadOKResponse(
        log_entry.metadata.seqnum,
        VECTOR_AS_SPAN(log_entry.user_tags),
        STRING_AS_SPAN(log_entry.data));
}
*/

}  // namespace

// Start handlers for local requests (from functions)

#define ONHOLD_IF_SEEN_FUTURE_VIEW(LOCAL_OP_VAR)                          \
    do {                                                                  \
        uint16_t view_id = log_utils::GetViewId(                          \
            (LOCAL_OP_VAR)->metalog_progress);                            \
        if (current_view_ == nullptr || view_id > current_view_->id()) {  \
            future_requests_.OnHoldRequest(                               \
                view_id, SharedLogRequest(LOCAL_OP_VAR));                 \
            return;                                                       \
        }                                                                 \
    } while (0)

void Engine::HandleLocalAppend(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::APPEND);
    HVLOG_F(1, "Handle local append: op_id={}, logspace={}, num_tags={}, size={}",
            op->id, op->user_logspace, op->user_tags.size(), op->data.length());
    const View* view = nullptr;
    LogMetaData log_metadata = MetaDataFromAppendOp(op);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (!current_view_active_) {
            HLOG(WARNING) << "Current view not active";
            FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
            return;
        }
        view = current_view_;
        uint32_t logspace_id = view->LogSpaceIdentifier(op->user_logspace);
        log_metadata.seqnum = bits::JoinTwo32(logspace_id, 0);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            locked_producer->LocalAppend(op, &log_metadata.localid);
        }
    }
    ReplicateLogEntry(view, log_metadata, VECTOR_AS_SPAN(op->user_tags), op->data.to_span());
}

void Engine::HandleLocalTrim(LocalOp* op) {
    DCHECK(op->type == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void Engine::HandleLocalRead(LocalOp* op) {
    DCHECK(  op->type == SharedLogOpType::READ_NEXT
          || op->type == SharedLogOpType::READ_PREV
          || op->type == SharedLogOpType::READ_NEXT_B);
    HVLOG_F(1, "Handle local read: op_id={}, logspace={}, tag={}, seqnum={}",
            op->id, op->user_logspace, op->query_tag, bits::HexStr0x(op->seqnum));
    onging_reads_.PutChecked(op->id, op);
    const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_SEEN_FUTURE_VIEW(op);
        uint32_t logspace_id = current_view_->LogSpaceIdentifier(op->user_logspace);
        sequencer_node = current_view_->GetSequencerNode(bits::LowHalf32(logspace_id));
        if (sequencer_node->IsIndexEngineNode(my_node_id())) {
            index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        }
    }
    bool use_local_index = true;
    if (absl::GetFlag(FLAGS_slog_engine_force_remote_index)) {
        use_local_index = false;
    }
    if (absl::GetFlag(FLAGS_slog_engine_prob_remote_index) > 0.0f) {
        float coin = utils::GetRandomFloat(0.0f, 1.0f);
        if (coin < absl::GetFlag(FLAGS_slog_engine_prob_remote_index)) {
            use_local_index = false;
        }
    }
    if (index_ptr != nullptr && use_local_index) {
        // Use local index
        IndexQuery query = BuildIndexQuery(op);
        Index::QueryResultVec query_results;
        {
            auto locked_index = index_ptr.Lock();
            locked_index->MakeQuery(query);
            locked_index->PollQueryResults(&query_results);
        }
        ProcessIndexQueryResults(query_results);
    } else {
        HVLOG_F(1, "There is no local index for sequencer {}, "
                   "will send request to remote engine node",
                DCHECK_NOTNULL(sequencer_node)->node_id());
        SharedLogMessage request = BuildReadRequestMessage(op);
        bool send_success = SendIndexReadRequest(DCHECK_NOTNULL(sequencer_node), &request);
        if (!send_success) {
            onging_reads_.RemoveChecked(op->id);
            FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
        }
    }
}

void Engine::HandleLocalSetAuxData(LocalOp* op) {
    uint64_t seqnum = op->seqnum;
    LogCachePutAuxData(seqnum, op->data.to_span());
    Message response = MessageHelper::NewSharedLogOpSucceeded(
        SharedLogResultType::AUXDATA_OK, seqnum);
    FinishLocalOpWithResponse(op, &response, /* metalog_progress= */ 0);
    if (!absl::GetFlag(FLAGS_slog_engine_propagate_auxdata)) {
        return;
    }
    if (auto log_entry = LogCacheGet(seqnum); log_entry.has_value()) {
        if (auto aux_data = LogCacheGetAuxData(seqnum); aux_data.has_value()) {
            uint16_t view_id = log_utils::GetViewId(seqnum);
            absl::ReaderMutexLock view_lk(&view_mu_);
            if (view_id < views_.size()) {
                const View* view = views_.at(view_id);
                PropagateAuxData(view, log_entry->metadata, *aux_data);
            }
        }
    }
}

#undef ONHOLD_IF_SEEN_FUTURE_VIEW

// Start handlers for remote messages

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
            HLOG(WARNING) << "Receive outdate request from view "   \
                          << (MESSAGE_VAR).view_id;                 \
            return;                                                 \
        }                                                           \
    } while (0)

void Engine::HandleRemoteRead(const SharedLogMessage& request) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(request);
    DCHECK(  op_type == SharedLogOpType::READ_NEXT
          || op_type == SharedLogOpType::READ_PREV
          || op_type == SharedLogOpType::READ_NEXT_B);
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(request, EMPTY_CHAR_SPAN);
        index_ptr = index_collection_.GetLogSpaceChecked(request.logspace_id);
    }
    IndexQuery query = BuildIndexQuery(request);
    Index::QueryResultVec query_results;
    {
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(&query_results);
    }
    ProcessIndexQueryResults(query_results);
}

void Engine::OnRecvNewMetaLogs(const SharedLogMessage& message,
                               std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), message.logspace_id);
    LogProducer::AppendResultVec append_results;
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto producer_ptr = producer_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_producer = producer_ptr.Lock();
            for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                locked_producer->ProvideMetaLog(metalog_proto);
            }
            locked_producer->PollAppendResults(&append_results);
        }
        if (current_view_->GetEngineNode(my_node_id())->HasIndexFor(message.sequencer_id)) {
            auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
            {
                auto locked_index = index_ptr.Lock();
                for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                    locked_index->ProvideMetaLog(metalog_proto);
                }
                locked_index->PollQueryResults(&query_results);
            }
        }
    }
    ProcessAppendResults(append_results);
    ProcessIndexQueryResults(query_results);
}

void Engine::OnRecvNewIndexData(const SharedLogMessage& message,
                                std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::INDEX_DATA);
    IndexDataProto index_data_proto;
    if (!index_data_proto.ParseFromArray(payload.data(),
                                         static_cast<int>(payload.size()))) {
        LOG(FATAL) << "Failed to parse IndexDataProto";
    }
    Index::QueryResultVec query_results;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        DCHECK(message.view_id < views_.size());
        const View* view = views_.at(message.view_id);
        if (!view->contains_engine_node(my_node_id())) {
            HLOG_F(FATAL, "View {} does not contain myself", view->id());
        }
        const View::Engine* engine_node = view->GetEngineNode(my_node_id());
        if (!engine_node->HasIndexFor(message.sequencer_id)) {
            HLOG_F(FATAL, "This node is not index node for log space {}",
                   bits::HexStr0x(message.logspace_id));
        }
        auto index_ptr = index_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_index = index_ptr.Lock();
            locked_index->ProvideIndexData(index_data_proto);
            locked_index->PollQueryResults(&query_results);
        }
    }
    ProcessIndexQueryResults(query_results);
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

void Engine::OnRecvResponse(const SharedLogMessage& message,
                            std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::RESPONSE);
    SharedLogResultType result = SharedLogMessageHelper::GetResultType(message);
    if (    result == SharedLogResultType::READ_OK
         || result == SharedLogResultType::EMPTY
         || result == SharedLogResultType::DATA_LOST) {
        uint64_t op_id = message.client_data;
        LocalOp* op;
        if (!onging_reads_.Poll(op_id, &op)) {
            HLOG_F(WARNING, "Cannot find read op with id {}", op_id);
            return;
        }
        if (result == SharedLogResultType::READ_OK) {
            uint64_t seqnum = bits::JoinTwo32(message.logspace_id, message.seqnum_lowhalf);
            HVLOG_F(1, "Receive remote read response for log (seqnum {})", bits::HexStr0x(seqnum));
            std::span<const uint64_t> user_tags;
            std::span<const char> log_data;
            std::span<const char> aux_data;
            log_utils::SplitPayloadForMessage(message, payload, &user_tags, &log_data, &aux_data);
            Message response;
            utils::AppendableBuffer aux_buffer;
            SerializeLogEntry(&response, &aux_buffer, seqnum, user_tags, log_data, aux_data);
            if (!aux_buffer.empty()) {
                uint64_t buf_id = NextAuxBufferId();
                MessageHelper::FillAuxBufferId(&response, buf_id);
                SendFuncWorkerAuxBuffer(op->client_id, buf_id, aux_buffer.to_span());
            }
/*
            Message response = BuildLocalReadOKResponse(seqnum, user_tags, log_data);
            if (aux_data.size() > 0) {
                response.log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
                MessageHelper::AppendInlineData(&response, aux_data);
            }
*/
            FinishLocalOpWithResponse(op, &response, message.user_metalog_progress);
            // Put the received log entry into log cache
            LogMetaData log_metadata = log_utils::GetMetaDataFromMessage(message);
            LogCachePut(log_metadata, user_tags, log_data);
            if (aux_data.size() > 0) {
                LogCachePutAuxData(seqnum, aux_data);
            }
        } else if (result == SharedLogResultType::EMPTY) {
            FinishLocalOpWithFailure(
                op, SharedLogResultType::EMPTY, message.user_metalog_progress);
        } else if (result == SharedLogResultType::DATA_LOST) {
            HLOG_F(WARNING, "Receive DATA_LOST response for read request: seqnum={}, tag={}",
                   bits::HexStr0x(op->seqnum), op->query_tag);
            FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
        } else {
            UNREACHABLE();
        }
    } else {
        HLOG(FATAL) << "Unknown result type: " << message.op_result;
    }
}

void Engine::ProcessAppendResults(const LogProducer::AppendResultVec& results) {
    for (const LogProducer::AppendResult& result : results) {
        LocalOp* op = reinterpret_cast<LocalOp*>(result.caller_data);
        if (result.seqnum != kInvalidLogSeqNum) {
            LogMetaData log_metadata = MetaDataFromAppendOp(op);
            log_metadata.seqnum = result.seqnum;
            log_metadata.localid = result.localid;
            LogCachePut(log_metadata, VECTOR_AS_SPAN(op->user_tags), op->data.to_span());
            Message response = MessageHelper::NewSharedLogOpSucceeded(
                SharedLogResultType::APPEND_OK, result.seqnum);
            FinishLocalOpWithResponse(op, &response, result.metalog_progress);
        } else {
            FinishLocalOpWithFailure(op, SharedLogResultType::DISCARDED);
        }
    }
}

void Engine::ProcessIndexFoundResult(const IndexQueryResult& query_result) {
    DCHECK(query_result.state == IndexQueryResult::kFound);
    const IndexQuery& query = query_result.original_query;
    bool local_request = (query.origin_node_id == my_node_id());
    uint64_t seqnum = query_result.found_result.seqnum;
    if (auto cached_log_entry = LogCacheGet(seqnum); cached_log_entry.has_value()) {
        // Cache hits
        HVLOG_F(1, "Cache hits for log entry (seqnum {})", bits::HexStr0x(seqnum));
        const LogEntry& log_entry = cached_log_entry.value();
        std::optional<std::string> cached_aux_data = LogCacheGetAuxData(seqnum);
        std::span<const char> aux_data;
        if (cached_aux_data.has_value()) {
/*
            size_t full_size = log_entry.data.size()
                             + log_entry.user_tags.size() * sizeof(uint64_t)
                             + cached_aux_data->size();
            if (full_size <= MESSAGE_INLINE_DATA_SIZE) {
                aux_data = STRING_AS_SPAN(*cached_aux_data);
            } else {
                HLOG_F(WARNING, "Inline buffer of message not large enough "
                                "for auxiliary data of log (seqnum {}): "
                                "log_size={}, num_tags={} aux_data_size={}",
                       bits::HexStr0x(seqnum), log_entry.data.size(),
                       log_entry.user_tags.size(), cached_aux_data->size());
            }
*/
            aux_data = STRING_AS_SPAN(*cached_aux_data);
        }
        if (local_request) {
            LocalOp* op = onging_reads_.PollChecked(query.client_data);
            Message response;
            utils::AppendableBuffer aux_buffer;
            SerializeLogEntry(
                &response, &aux_buffer,
                seqnum, VECTOR_AS_SPAN(log_entry.user_tags),
                STRING_AS_SPAN(log_entry.data), aux_data);
            if (!aux_buffer.empty()) {
                uint64_t buf_id = NextAuxBufferId();
                MessageHelper::FillAuxBufferId(&response, buf_id);
                SendFuncWorkerAuxBuffer(op->client_id, buf_id, aux_buffer.to_span());
            }
/*
            Message response = BuildLocalReadOKResponse(log_entry);
            response.log_aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
            MessageHelper::AppendInlineData(&response, aux_data);
*/
            FinishLocalOpWithResponse(op, &response, query_result.metalog_progress);
        } else {
            HVLOG_F(1, "Send read response for log (seqnum {})", bits::HexStr0x(seqnum));
            SharedLogMessage response = SharedLogMessageHelper::NewReadOkResponse();
            log_utils::PopulateMetaDataToMessage(log_entry.metadata, &response);
            response.user_metalog_progress = query_result.metalog_progress;
            response.aux_data_size = gsl::narrow_cast<uint16_t>(aux_data.size());
            SendReadResponse(query, &response,
                             VECTOR_AS_CHAR_SPAN(log_entry.user_tags),
                             STRING_AS_SPAN(log_entry.data), aux_data);
        }
    } else {
        // Cache miss
        const View::Engine* engine_node = nullptr;
        {
            absl::ReaderMutexLock view_lk(&view_mu_);
            uint16_t view_id = query_result.found_result.view_id;
            if (view_id < views_.size()) {
                const View* view = views_.at(view_id);
                engine_node = view->GetEngineNode(query_result.found_result.engine_id);
            } else {
                HLOG_F(FATAL, "Cannot find view {}", view_id);
            }
        }
        bool success = SendStorageReadRequest(query_result, engine_node);
        if (!success) {
            HLOG_F(WARNING, "Failed to send read request for seqnum {} ", bits::HexStr0x(seqnum));
            if (local_request) {
                LocalOp* op = onging_reads_.PollChecked(query.client_data);
                FinishLocalOpWithFailure(op, SharedLogResultType::DATA_LOST);
            } else {
                SendReadFailureResponse(query, SharedLogResultType::DATA_LOST);
            }
        }
    }
}

void Engine::ProcessIndexContinueResult(const IndexQueryResult& query_result,
                                        Index::QueryResultVec* more_results) {
    DCHECK(query_result.state == IndexQueryResult::kContinue);
    HVLOG_F(1, "Process IndexContinueResult: next_view_id={}",
            query_result.next_view_id);
    const IndexQuery& query = query_result.original_query;
    const View::Sequencer* sequencer_node = nullptr;
    LockablePtr<Index> index_ptr;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        uint16_t view_id = query_result.next_view_id;
        if (view_id >= views_.size()) {
            HLOG_F(FATAL, "Cannot find view {}", view_id);
        }
        const View* view = views_.at(view_id);
        uint32_t logspace_id = view->LogSpaceIdentifier(query.user_logspace);
        sequencer_node = view->GetSequencerNode(bits::LowHalf32(logspace_id));
        if (sequencer_node->IsIndexEngineNode(my_node_id())) {
            index_ptr = index_collection_.GetLogSpaceChecked(logspace_id);
        }
    }
    if (index_ptr != nullptr) {
        HVLOG(1) << "Use local index";
        IndexQuery query = BuildIndexQuery(query_result);
        auto locked_index = index_ptr.Lock();
        locked_index->MakeQuery(query);
        locked_index->PollQueryResults(more_results);
    } else {
        HVLOG(1) << "Send to remote index";
        SharedLogMessage request = BuildReadRequestMessage(query_result);
        bool send_success = SendIndexReadRequest(DCHECK_NOTNULL(sequencer_node), &request);
        if (!send_success) {
            uint32_t logspace_id = bits::JoinTwo16(sequencer_node->view()->id(),
                                                   sequencer_node->node_id());
            HLOG_F(ERROR, "Failed to send read index request for logspace {}",
                   bits::HexStr0x(logspace_id));
        }
    }
}

void Engine::ProcessIndexQueryResults(const Index::QueryResultVec& results) {
    Index::QueryResultVec more_results;
    for (const IndexQueryResult& result : results) {
        const IndexQuery& query = result.original_query;
        switch (result.state) {
        case IndexQueryResult::kFound:
            ProcessIndexFoundResult(result);
            break;
        case IndexQueryResult::kEmpty:
            if (query.origin_node_id == my_node_id()) {
                FinishLocalOpWithFailure(
                    onging_reads_.PollChecked(query.client_data),
                    SharedLogResultType::EMPTY, result.metalog_progress);
            } else {
                SendReadFailureResponse(
                    query, SharedLogResultType::EMPTY, result.metalog_progress);
            }
            break;
        case IndexQueryResult::kContinue:
            ProcessIndexContinueResult(result, &more_results);
            break;
        default:
            UNREACHABLE();
        }
    }
    if (!more_results.empty()) {
        ProcessIndexQueryResults(more_results);
    }
}

void Engine::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        if (request.local_op == nullptr) {
            MessageHandler(request.message, STRING_AS_SPAN(request.payload));
        } else {
            LocalOpHandler(reinterpret_cast<LocalOp*>(request.local_op));
        }
    }
}

SharedLogMessage Engine::BuildReadRequestMessage(LocalOp* op) {
    DCHECK(  op->type == SharedLogOpType::READ_NEXT
          || op->type == SharedLogOpType::READ_PREV
          || op->type == SharedLogOpType::READ_NEXT_B);
    SharedLogMessage request = SharedLogMessageHelper::NewReadMessage(op->type);
    request.origin_node_id = my_node_id();
    request.hop_times = 1;
    request.client_data = op->id;
    request.user_logspace = op->user_logspace;
    request.query_tag = op->query_tag;
    request.query_seqnum = op->seqnum;
    request.user_metalog_progress = op->metalog_progress;
    request.flags |= protocol::kReadInitialFlag;
    request.prev_view_id = 0;
    request.prev_engine_id = 0;
    request.prev_found_seqnum = kInvalidLogSeqNum;
    return request;
}

SharedLogMessage Engine::BuildReadRequestMessage(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kContinue);
    IndexQuery query = result.original_query;
    SharedLogMessage request = SharedLogMessageHelper::NewReadMessage(
        query.DirectionToOpType());
    request.origin_node_id = query.origin_node_id;
    request.hop_times = query.hop_times + 1;
    request.client_data = query.client_data;
    request.user_logspace = query.user_logspace;
    request.query_tag = query.user_tag;
    request.query_seqnum = query.query_seqnum;
    request.user_metalog_progress = result.metalog_progress;
    request.prev_view_id = result.found_result.view_id;
    request.prev_engine_id = result.found_result.engine_id;
    request.prev_found_seqnum = result.found_result.seqnum;
    return request;
}

IndexQuery Engine::BuildIndexQuery(LocalOp* op) {
    return IndexQuery {
        .direction = IndexQuery::DirectionFromOpType(op->type),
        .origin_node_id = my_node_id(),
        .hop_times = 0,
        .initial = true,
        .client_data = op->id,
        .user_logspace = op->user_logspace,
        .user_tag = op->query_tag,
        .query_seqnum = op->seqnum,
        .metalog_progress = op->metalog_progress,
        .prev_found_result = {
            .view_id = 0,
            .engine_id = 0,
            .seqnum = kInvalidLogSeqNum
        }
    };
}

IndexQuery Engine::BuildIndexQuery(const SharedLogMessage& message) {
    SharedLogOpType op_type = SharedLogMessageHelper::GetOpType(message);
    return IndexQuery {
        .direction = IndexQuery::DirectionFromOpType(op_type),
        .origin_node_id = message.origin_node_id,
        .hop_times = message.hop_times,
        .initial = (message.flags | protocol::kReadInitialFlag) != 0,
        .client_data = message.client_data,
        .user_logspace = message.user_logspace,
        .user_tag = message.query_tag,
        .query_seqnum = message.query_seqnum,
        .metalog_progress = message.user_metalog_progress,
        .prev_found_result = IndexFoundResult {
            .view_id = message.prev_view_id,
            .engine_id = message.prev_engine_id,
            .seqnum = message.prev_found_seqnum
        }
    };
}

IndexQuery Engine::BuildIndexQuery(const IndexQueryResult& result) {
    DCHECK(result.state == IndexQueryResult::kContinue);
    IndexQuery query = result.original_query;
    query.initial = false;
    query.metalog_progress = result.metalog_progress;
    query.prev_found_result = result.found_result;
    return query;
}

}  // namespace log
}  // namespace faas
