#pragma once

#include "log/common.h"
#include "log/view.h"
#include "log/log_space.h"
#include "utils/bits.h"
#include "utils/lockable_ptr.h"

namespace faas {
namespace log {

template<class T>
class LogSpaceCollection {
public:
    LogSpaceCollection();
    ~LogSpaceCollection();

    bool is_from_future_view(uint32_t identifier) const {
        return bits::HighHalf32(identifier) >= next_view_id_;
    }
    bool is_from_current_view(uint32_t identifier) const {
        return bits::HighHalf32(identifier) + 1 == next_view_id_;
    }

    LockablePtr<T> GetLogSpace(uint32_t identifier) const;

    void OnNewView(const View* view, std::vector<std::unique_ptr<T>> log_spaces);
    // Only active LogSpace can be finalized
    bool FinalizeLogSpace(uint32_t identifier);
    // Only finalized LogSpace can be removed
    bool RemoveLogSpace(uint32_t identifier);

    bool GetAllActiveLogSpaces(uint16_t view_id, std::vector<LockablePtr<T>>* results) const;
    LockablePtr<T> GetNextActiveLogSpace(uint32_t min_identifier) const;
    LockablePtr<T> GetNextFinalizedLogSpace(uint32_t min_identifier) const;

private:
    uint16_t next_view_id_;

    std::set</* identifier */ uint32_t> active_log_spaces_;
    std::set</* identifier */ uint32_t> finalized_log_spaces_;

    absl::flat_hash_map</* identifier */ uint32_t, LockablePtr<T>> log_spaces_;

    DISALLOW_COPY_AND_ASSIGN(LogSpaceCollection);
};

// Used for on-holding requests for future views
class FutureRequests {
public:
    FutureRequests();
    ~FutureRequests();

    void OnNewView(const View* view, std::vector<SharedLogRequest>* ready_requests);
    void OnHoldRequest(SharedLogRequest request);

private:
    uint16_t next_view_id_;

    absl::flat_hash_map</* view_id */ uint16_t, std::vector<SharedLogRequest>>
        onhold_requests_;

    DISALLOW_COPY_AND_ASSIGN(FutureRequests);
};

}  // namespace log
}  // namespace faas

#include "log/utils-inl.h"
