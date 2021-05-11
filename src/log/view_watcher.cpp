#include "log/view_watcher.h"

#include "utils/bits.h"

#define log_header_ "ViewWatcher: "

namespace faas {
namespace log {

ViewWatcher::ViewWatcher() {}

ViewWatcher::~ViewWatcher() {}

void ViewWatcher::StartWatching(zk::ZKSession* session) {
    watcher_.emplace(session, "view", /* sequential_znodes= */ true);
    watcher_->SetNodeCreatedCallback(
        absl::bind_front(&ViewWatcher::OnZNodeCreated, this));
    watcher_->Start();
}

void ViewWatcher::SetViewCreatedCallback(ViewCallback cb) {
    view_created_cb_ = cb;
}

void ViewWatcher::SetViewFrozenCallback(ViewCallback cb) {
    view_frozen_cb_ = cb;
}

void ViewWatcher::SetViewFinalizedCallback(ViewFinalizedCallback cb) {
    view_finalized_cb_ = cb;
}

void ViewWatcher::InstallNextView(const ViewProto& view_proto) {
    if (view_proto.view_id() != next_view_id()) {
        HLOG_F(FATAL, "Non-consecutive view_id {}", view_proto.view_id());
    }
    View* view = new View(view_proto);
    views_.emplace_back(view);
    if (view_created_cb_) {
        view_created_cb_(view);
    }
}

void ViewWatcher::FinalizeCurrentView(const FinalizedViewProto& finalized_view_proto) {
    const View* view = current_view();
    if (view == nullptr || finalized_view_proto.view_id() != view->id()) {
        LOG_F(FATAL, "Cannot finalized current view: view_id {}",
              finalized_view_proto.view_id());
    }
    FinalizedView* finalized_view = new FinalizedView(view, finalized_view_proto);
    finalized_views_.emplace_back(finalized_view);
    if (view_finalized_cb_) {
        view_finalized_cb_(finalized_view);
    }
}

void ViewWatcher::OnZNodeCreated(std::string_view path, std::span<const char> contents) {
    if (absl::StartsWith(path, "new")) {
        ViewProto view_proto;
        if (!view_proto.ParseFromArray(contents.data(),
                                       static_cast<int>(contents.size()))) {
            HLOG(FATAL) << "Failed to parse ViewProto";
        }
        InstallNextView(view_proto);
    } else if (absl::StartsWith(path, "freeze")) {
        int parsed;
        if (!absl::SimpleAtoi(std::string_view(contents.data(), contents.size()), &parsed)) {
            HLOG(FATAL) << "Failed to parse view ID";
        }
        const View* view = view_with_id(gsl::narrow_cast<uint16_t>(parsed));
        if (view == nullptr) {
            HLOG_F(FATAL, "Invalid view_id {}", parsed);
        }
        if (view_frozen_cb_) {
            view_frozen_cb_(view);
        }
    } else if (absl::StartsWith(path, "finalize")) {
        FinalizedViewProto finalized_view_proto;
        if (!finalized_view_proto.ParseFromArray(contents.data(),
                                                 static_cast<int>(contents.size()))) {
            HLOG(FATAL) << "Failed to parse FinalizedViewProto";
        }
        FinalizeCurrentView(finalized_view_proto);
    } else {
        HLOG_F(FATAL, "Unknown znode path {}", path);
    }
}

FinalizedView::FinalizedView(const View* view,
                             const FinalizedViewProto& finalized_view_proto)
    : view_(view) {
    DCHECK_EQ(gsl::narrow_cast<size_t>(finalized_view_proto.metalog_positions_size()),
              view->num_phylogs());
    DCHECK_EQ(gsl::narrow_cast<size_t>(finalized_view_proto.tail_metalogs_size()),
              view->num_phylogs());
    size_t idx = 0;
    for (uint16_t sequencer_id : view->GetSequencerNodes()) {
        if (!view->is_active_phylog(sequencer_id)) {
            continue;
        }
        uint32_t logspace_id = bits::JoinTwo16(view_->id(), sequencer_id);
        final_metalog_positions_[logspace_id] = finalized_view_proto.metalog_positions(
            static_cast<int>(idx));
        std::vector<MetaLogProto> metalogs;
        const auto& tail_metalog_proto = finalized_view_proto.tail_metalogs(
            static_cast<int>(idx));
        DCHECK_EQ(logspace_id, tail_metalog_proto.logspace_id());
        for (const MetaLogProto& metalog : tail_metalog_proto.metalogs()) {
            metalogs.push_back(metalog);
        }
        tail_metalogs_[logspace_id] = std::move(metalogs);
        idx++;
    }
}

}  // namespace log
}  // namespace faas
