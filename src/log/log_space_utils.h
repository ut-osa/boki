#pragma once

#include "log/common.h"
#include "log/view.h"
#include "log/log_space.h"
#include "utils/lockable_ptr.h"

namespace faas {
namespace log {

template<class T>
class LogSpaceCollection {
public:
    LogSpaceCollection() {}
    ~LogSpaceCollection() {}

    bool is_from_future_view(uint32_t logspace_id) const;
    bool is_from_current_view(uint32_t logspace_id) const;

    void OnNewView(const View* view, std::vector<std::unique_ptr<T>> log_spaces);
    void OnLogSpaceFinalized(uint32_t logspace_id);

    // Only finalized LogSpace can be removed
    void RemoveLogSpace(uint32_t logspace_id);

    LockablePtr<T> GetLogSpace(uint32_t logspace_id) const;
    std::vector<LockablePtr<T>> GetAllLogSpaces(uint16_t view_id) const;

    LockablePtr<T> OldestFinalizedLogSpace() const;
    LockablePtr<T> OldestActiveLogSpace() const;

private:
    DISALLOW_COPY_AND_ASSIGN(LogSpaceCollection);
};

class RequestsForFutureView {
public:
    void OnNewView(const View* view, std::vector<SharedLogRequest>* ready_requests);
    void OnHoldRequest(const protocol::SharedLogMessage& message,
                       std::span<const char> payload);

private:
    DISALLOW_COPY_AND_ASSIGN(RequestsForFutureView);
};

}  // namespace log
}  // namespace faas
