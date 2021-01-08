#pragma once

#include "base/common.h"
#include "common/zk.h"
#include "common/zk_utils.h"
#include "proto/shared_log.pb.h"
#include "log/view.h"

namespace faas {
namespace log {

class FinalizedView;

class ViewWatcher {
public:
    ViewWatcher();
    ~ViewWatcher();

    void StartWatching(zk::ZKSession* session);

    typedef std::function<void(const View*)> ViewCallback;
    void SetViewCreatedCallback(ViewCallback cb);
    void SetViewFrozenCallback(ViewCallback cb);

    typedef std::function<void(const FinalizedView*)> ViewFinalizedCallback;
    void SetViewFinalizedCallback(ViewFinalizedCallback cb);

    uint16_t next_view_id() const {
        return gsl::narrow_cast<uint16_t>(views_.size());
    }

    const View* current_view() const {
        return views_.empty() ? nullptr : views_.back().get();
    }

    const View* view_with_id(uint16_t view_id) const {
        return view_id < views_.size() ? views_.at(view_id).get() : nullptr;
    }

private:
    std::unique_ptr<zk_utils::DirWatcher> watcher_;

    std::vector<std::unique_ptr<View>> views_;
    std::vector<std::unique_ptr<FinalizedView>> finalized_views_;

    ViewCallback          view_created_cb_;
    ViewCallback          view_frozen_cb_;
    ViewFinalizedCallback view_finalized_cb_;

    void InstallNextView(const ViewProto& view_proto);
    void FinalizeCurrentView(const FinalizedViewProto& finalized_view_proto);
    void OnZNodeCreated(std::string_view path, std::span<const char> contents);

    DISALLOW_COPY_AND_ASSIGN(ViewWatcher);
};

class FinalizedView {
public:
    FinalizedView(const View* view, const FinalizedViewProto& finalized_view_proto);
    ~FinalizedView() {}

    const View* view() const { return view_; }

    uint32_t final_metalog_position(uint16_t sequencer_id) const {
        DCHECK(final_metalog_positions_.contains(sequencer_id));
        return final_metalog_positions_.at(sequencer_id);
    }

    typedef std::vector<MetaLogProto> MetaLogVec;
    const MetaLogVec& tail_metalogs(uint16_t sequencer_id) const {
        DCHECK(tail_metalogs_.contains(sequencer_id));
        return tail_metalogs_.at(sequencer_id);
    }

private:
    const View* view_;
    absl::flat_hash_map<uint16_t, uint32_t> final_metalog_positions_;
    absl::flat_hash_map<uint16_t, MetaLogVec> tail_metalogs_;

    DISALLOW_COPY_AND_ASSIGN(FinalizedView);
};

}  // namespace log
}  // namespace faas
