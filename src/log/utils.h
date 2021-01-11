#pragma once

#include "log/common.h"
#include "log/view.h"

namespace faas {
namespace log {

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
