#pragma once

#include "base/common.h"
#include "common/zk.h"
#include "common/zk_utils.h"
#include "log/view.h"

__BEGIN_THIRD_PARTY_HEADERS
#include "proto/shared_log.pb.h"
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace log {

class FinalizedView;

class ViewWatcher {
public:
    ViewWatcher() = default;
    ~ViewWatcher() = default;

    void StartWatching(zk::ZKSession* session);

    using ViewCallback = std::function<void(const View*)>;
    void SetViewCreatedCallback(ViewCallback cb);
    void SetViewFrozenCallback(ViewCallback cb);

    using ViewFinalizedCallback = std::function<void(const FinalizedView*)>;
    void SetViewFinalizedCallback(ViewFinalizedCallback cb);

private:
    std::optional<zk_utils::DirWatcher> watcher_;

    std::vector<std::unique_ptr<View>> views_;
    std::vector<std::unique_ptr<FinalizedView>> finalized_views_;

    ViewCallback          view_created_cb_;
    ViewCallback          view_frozen_cb_;
    ViewFinalizedCallback view_finalized_cb_;

    inline uint16_t next_view_id() const {
        return gsl::narrow_cast<uint16_t>(views_.size());
    }

    inline const View* current_view() const {
        return views_.empty() ? nullptr : views_.back().get();
    }

    inline const View* view_with_id(uint16_t view_id) const {
        return view_id < views_.size() ? views_.at(view_id).get() : nullptr;
    }

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

    uint32_t final_metalog_position(uint32_t logspace_id) const {
        DCHECK(final_metalog_positions_.contains(logspace_id));
        return final_metalog_positions_.at(logspace_id);
    }

    using MetaLogVec = std::vector<MetaLogProto>;
    const MetaLogVec& tail_metalogs(uint32_t logspace_id) const {
        DCHECK(tail_metalogs_.contains(logspace_id));
        return tail_metalogs_.at(logspace_id);
    }

private:
    const View* view_;
    absl::flat_hash_map<uint32_t, uint32_t> final_metalog_positions_;
    absl::flat_hash_map<uint32_t, MetaLogVec> tail_metalogs_;

    DISALLOW_COPY_AND_ASSIGN(FinalizedView);
};

}  // namespace log
}  // namespace faas
