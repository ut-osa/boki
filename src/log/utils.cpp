#include "log/utils.h"

namespace faas {
namespace log {

FutureRequests::FutureRequests() {}

FutureRequests::~FutureRequests() {}

void FutureRequests::OnNewView(const View* view,
                               std::vector<SharedLogRequest>* ready_requests) {
    if (view->id() != next_view_id_) {
        LOG(FATAL) << fmt::format("Views are not consecutive: have={}, expect={}",
                                  view->id(), next_view_id_);
    }
    if (onhold_requests_.contains(view->id())) {
        *ready_requests = std::move(onhold_requests_[view->id()]);
        onhold_requests_.erase(view->id());
    }
    next_view_id_++;
}

void FutureRequests::OnHoldRequest(SharedLogRequest request) {
    uint16_t view_id = request.message.view_id;
    if (view_id < next_view_id_) {
        LOG(FATAL) << fmt::format("Receive request from view not in the future: "
                                  "request_view_id={}, next_view_id={}",
                                  view_id, next_view_id_);
    }
    onhold_requests_[view_id].push_back(std::move(request));
}

}  // namespace log
}  // namespace faas
