#include "log/sequencer.h"

#include "server/constants.h"
#include "log/flags.h"
#include "utils/bits.h"

namespace faas {
namespace log {

using protocol::SharedLogMessage;
using protocol::SharedLogMessageHelper;
using protocol::SharedLogOpType;

Sequencer::Sequencer(uint16_t node_id)
    : SequencerBase(node_id),
      log_header_(fmt::format("Sequencer[{}-N]: ", node_id)),
      current_view_(nullptr) {}

Sequencer::~Sequencer() {}

void Sequencer::OnViewCreated(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "New view {} created", view->id());
    bool contains_myself = view->contains_sequencer_node(my_node_id());
    if (!contains_myself) {
        HLOG_F(WARNING, "View {} does not include myself", view->id());
    }
    std::vector<SharedLogRequest> ready_requests;
    {
        absl::MutexLock view_lk(&view_mu_);
        if (contains_myself) {
            if (view->is_active_phylog(my_node_id())) {
                primary_collection_.InstallLogSpace(
                    std::make_unique<MetaLogPrimary>(view, my_node_id()));
            }
            for (uint16_t id : view->GetSequencerNodes()) {
                if (!view->is_active_phylog(id)) {
                    continue;
                }
                if (view->GetSequencerNode(id)->IsReplicaSequencerNode(my_node_id())) {
                    backup_collection_.InstallLogSpace(
                        std::make_unique<MetaLogBackup>(view, id));
                }
            }
        }
        current_primary_ = primary_collection_.GetLogSpace(
            bits::JoinTwo16(view->id(), my_node_id()));
        future_requests_.OnNewView(view, contains_myself ? &ready_requests : nullptr);
        current_view_ = view;
        log_header_ = fmt::format("Sequencer[{}-{}]: ", my_node_id(), view->id());
    }
    if (!ready_requests.empty()) {
        HLOG_F(INFO, "{} requests for the new view", ready_requests.size());
        SomeIOWorker()->ScheduleFunction(
            nullptr, [this, requests = std::move(ready_requests)] {
                ProcessRequests(requests);
            }
        );
    }
}

namespace {
template<class T>
void FreezeLogSpace(LockablePtr<T> logspace_ptr, MetaLogsProto* tail_metalogs) {
    auto locked_logspace = logspace_ptr.Lock();
    locked_logspace->Freeze();
    tail_metalogs->set_logspace_id(locked_logspace->identifier());
    uint32_t num_entries = gsl::narrow_cast<uint32_t>(
        absl::GetFlag(FLAGS_slog_num_tail_metalog_entries));
    uint32_t end_pos = locked_logspace->metalog_position();
    uint32_t start_pos = end_pos - std::min(num_entries, end_pos);
    for (uint32_t pos = start_pos; pos < end_pos; pos++) {
        auto metalog = locked_logspace->GetMetaLog(pos);
        CHECK(metalog.has_value());
        tail_metalogs->add_metalogs()->CopyFrom(*metalog);
    }
}
}  // namespace

void Sequencer::OnViewFrozen(const View* view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} frozen", view->id());
    FrozenSequencerProto frozen_proto;
    frozen_proto.set_view_id(view->id());
    frozen_proto.set_sequencer_id(my_node_id());
    {
        absl::MutexLock view_lk(&view_mu_);
        DCHECK_EQ(view->id(), current_view_->id());
        if (current_primary_ != nullptr) {
            FreezeLogSpace<MetaLogPrimary>(current_primary_, frozen_proto.add_tail_metalogs());
        }
        backup_collection_.ForEachActiveLogSpace(
            view, [&frozen_proto] (uint32_t, LockablePtr<MetaLogBackup> logspace_ptr) {
                FreezeLogSpace<MetaLogBackup>(std::move(logspace_ptr),
                                              frozen_proto.add_tail_metalogs());
            }
        );
    }
    if (frozen_proto.tail_metalogs().empty()) {
        return;
    }
    std::string serialized;
    CHECK(frozen_proto.SerializeToString(&serialized));
    zk_session()->Create(
        fmt::format("freeze/{}-{}", view->id(), my_node_id()),
        STRING_AS_SPAN(serialized),
        zk::ZKCreateMode::kPersistentSequential,
        [this] (zk::ZKStatus status, const zk::ZKResult& result, bool*) {
            if (!status.ok()) {
                HLOG(FATAL) << "Failed to publish freeze data: " << status.ToString();
            }
            HLOG_F(INFO, "Frozen at ZK path {}", result.path);
        }
    );
}

void Sequencer::OnViewFinalized(const FinalizedView* finalized_view) {
    DCHECK(zk_session()->WithinMyEventLoopThread());
    HLOG_F(INFO, "View {} finalized", finalized_view->view()->id());
    absl::MutexLock view_lk(&view_mu_);
    DCHECK_EQ(finalized_view->view()->id(), current_view_->id());
    if (current_primary_ != nullptr) {
        log_utils::FinalizedLogSpace<MetaLogPrimary>(current_primary_, finalized_view);
    }
    backup_collection_.ForEachActiveLogSpace(
        finalized_view->view(),
        [finalized_view] (uint32_t, LockablePtr<MetaLogBackup> logspace_ptr) {
            log_utils::FinalizedLogSpace<MetaLogBackup>(std::move(logspace_ptr), finalized_view);
        }
    );
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

#define PANIC_IF_FROM_FUTURE_VIEW(MESSAGE_VAR)                      \
    do {                                                            \
        if (current_view_ == nullptr                                \
                || (MESSAGE_VAR).view_id > current_view_->id()) {   \
            HLOG_F(FATAL, "Receive message from future view {}",    \
                   (MESSAGE_VAR).view_id);                          \
        }                                                           \
    } while (0)

#define IGNORE_IF_FROM_PAST_VIEW(MESSAGE_VAR)                       \
    do {                                                            \
        if (current_view_ != nullptr                                \
                && (MESSAGE_VAR).view_id < current_view_->id()) {   \
            HLOG_F(WARNING, "Receive outdate message from view {}", \
                   (MESSAGE_VAR).view_id);                          \
            return;                                                 \
        }                                                           \
    } while (0)

#define RETURN_IF_LOGSPACE_INACTIVE(LOGSPACE_PTR)                   \
    do {                                                            \
        if ((LOGSPACE_PTR)->frozen()) {                             \
            uint32_t logspace_id = (LOGSPACE_PTR)->identifier();    \
            HLOG_F(WARNING, "LogSpace {} is frozen",                \
                   bits::HexStr0x(logspace_id));                    \
            return;                                                 \
        }                                                           \
        if ((LOGSPACE_PTR)->finalized()) {                          \
            uint32_t logspace_id = (LOGSPACE_PTR)->identifier();    \
            HLOG_F(WARNING, "LogSpace {} is finalized",             \
                   bits::HexStr0x(logspace_id));                    \
            return;                                                 \
        }                                                           \
    } while (0)

void Sequencer::HandleTrimRequest(const SharedLogMessage& request) {
    DCHECK(SharedLogMessageHelper::GetOpType(request) == SharedLogOpType::TRIM);
    NOT_IMPLEMENTED();
}

void Sequencer::OnRecvMetaLogProgress(const SharedLogMessage& message) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::META_PROG);
    const View* view = nullptr;
    LogSpaceBase::MetaLogProtoVec replicated_metalogs;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        PANIC_IF_FROM_FUTURE_VIEW(message);  // I believe this will never happen
        IGNORE_IF_FROM_PAST_VIEW(message);
        view = current_view_;
        auto logspace_ptr = primary_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_logspace = logspace_ptr.Lock();
            RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
            locked_logspace->UpdateReplicaProgress(
                message.origin_node_id, message.metalog_position,
                &replicated_metalogs);
        }
    }
    PropagateMetaLogs(DCHECK_NOTNULL(view), replicated_metalogs);
}

void Sequencer::OnRecvShardProgress(const SharedLogMessage& message,
                                    std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::SHARD_PROG);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
        auto logspace_ptr = primary_collection_.GetLogSpaceChecked(message.logspace_id);
        {
            auto locked_logspace = logspace_ptr.Lock();
            RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
            std::vector<uint32_t> progress(payload.size() / sizeof(uint32_t), 0);
            memcpy(progress.data(), payload.data(), payload.size());
            locked_logspace->UpdateStorageProgress(message.origin_node_id, progress);
        }
    }
}

void Sequencer::OnRecvNewMetaLogs(const SharedLogMessage& message,
                                  std::span<const char> payload) {
    DCHECK(SharedLogMessageHelper::GetOpType(message) == SharedLogOpType::METALOGS);
    uint32_t logspace_id = message.logspace_id;
    MetaLogsProto metalogs_proto = log_utils::MetaLogsFromPayload(payload);
    DCHECK_EQ(metalogs_proto.logspace_id(), logspace_id);
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        ONHOLD_IF_FROM_FUTURE_VIEW(message, payload);
        IGNORE_IF_FROM_PAST_VIEW(message);
    }
    if (journal_enabled()) {
        CurrentIOWorkerChecked()->JournalAppend(
            kMetalogBackupJournalRecordType, payload,
            absl::bind_front(&Sequencer::StoreMetaLogAsBackup, this, metalogs_proto));
    } else {
        StoreMetaLogAsBackup(std::move(metalogs_proto));
    }
}

#undef ONHOLD_IF_FROM_FUTURE_VIEW
#undef PANIC_IF_FROM_FUTURE_VIEW
#undef IGNORE_IF_FROM_PAST_VIEW

void Sequencer::ProcessRequests(const std::vector<SharedLogRequest>& requests) {
    for (const SharedLogRequest& request : requests) {
        MessageHandler(request.message, STRING_AS_SPAN(request.payload));
    }
}

void Sequencer::PropagateMetaLogs(const View* view,
                                  const LogSpaceBase::MetaLogProtoVec& metalogs) {
    for (const MetaLogProto& metalog_proto : metalogs) {
        PropagateMetaLog(view, metalog_proto);
    }
}

void Sequencer::MarkNextCutIfDoable() {
    const View* view = nullptr;
    MetaLogProto meta_log_proto;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        if (current_primary_ == nullptr || current_view_ == nullptr) {
            return;
        }
        view = current_view_;
        {
            auto locked_logspace = current_primary_.Lock();
            RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
            if (!locked_logspace->all_metalog_replicated()) {
                HLOG(INFO) << "Not all meta log replicated, will not mark new cut";
                return;
            }
            if (auto next_cut = locked_logspace->MarkNextCut(); next_cut.has_value()) {
                meta_log_proto = std::move(*next_cut);
            } else {
                return;
            }
            if (!journal_enabled()) {
                locked_logspace->UpdateReplicaProgress(
                    locked_logspace->sequencer_id(),
                    meta_log_proto.metalog_seqnum() + 1,
                    /* newly_replicated_metalogs= */ nullptr);
            }
        }
    }
    ReplicateMetaLog(DCHECK_NOTNULL(view), meta_log_proto);
    if (!journal_enabled()) {
        return;
    }
    std::string serialized;
    CHECK(meta_log_proto.SerializeToString(&serialized));
    CurrentIOWorkerChecked()->JournalAppend(
        kMetalogPrimaryJournalRecordType, STRING_AS_SPAN(serialized),
        [this, view, metalog_seqnum = meta_log_proto.metalog_seqnum()] () {
            absl::ReaderMutexLock view_lk(&view_mu_);
            if (current_view_ != view) {
                return;
            }
            LogSpaceBase::MetaLogProtoVec replicated_metalogs;
            {
                auto locked_logspace = current_primary_.Lock();
                RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
                locked_logspace->UpdateReplicaProgress(
                    locked_logspace->sequencer_id(), metalog_seqnum + 1,
                    &replicated_metalogs);
            }
            PropagateMetaLogs(view, replicated_metalogs);
        }
    );
}

void Sequencer::StoreMetaLogAsBackup(MetaLogsProto metalogs_proto) {
    uint32_t logspace_id = metalogs_proto.logspace_id();
    uint32_t old_metalog_position;
    uint32_t new_metalog_position;
    {
        absl::ReaderMutexLock view_lk(&view_mu_);
        auto logspace_ptr = backup_collection_.GetLogSpaceChecked(logspace_id);
        {
            auto locked_logspace = logspace_ptr.Lock();
            RETURN_IF_LOGSPACE_INACTIVE(locked_logspace);
            old_metalog_position = locked_logspace->metalog_position();
            for (const MetaLogProto& metalog_proto : metalogs_proto.metalogs()) {
                locked_logspace->ProvideMetaLog(metalog_proto);
            }
            new_metalog_position = locked_logspace->metalog_position();
        }
    }
    if (new_metalog_position > old_metalog_position) {
        SharedLogMessage response = SharedLogMessageHelper::NewMetaLogProgressMessage(
            logspace_id, new_metalog_position);
        uint16_t sequencer_id = bits::LowHalf32(logspace_id);
        SendSequencerMessage(sequencer_id, &response);
    }
}

#undef RETURN_IF_LOGSPACE_INACTIVE

}  // namespace log
}  // namespace faas
