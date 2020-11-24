#include "common/uv.h"

#include "utils/io.h"
#include "utils/random.h"

namespace faas {
namespace uv {

void HandleFreeCallback(uv_handle_t* handle) {
    free(handle);
}

HandleScope::HandleScope()
    : loop_(nullptr), num_handles_on_closing_(0) {}

HandleScope::~HandleScope() {
    DCHECK(handles_.empty());
}

void HandleScope::Init(uv_loop_t* loop, std::function<void()> finish_callback) {
    DCHECK(loop_ == nullptr);
    loop_ = loop;
    finish_callback_ = finish_callback;
}

void HandleScope::AddHandle(uv_handle_t* handle) {
    DCHECK(handle->loop == loop_);
    handles_.insert(handle);
}

void HandleScope::CloseHandle(uv_handle_t* handle) {
    DCHECK_IN_EVENT_LOOP_THREAD(loop_);
    DCHECK(handles_.contains(handle));
    handles_.erase(handle);
    handle->data = this;
    num_handles_on_closing_++;
    uv_close(handle, &HandleScope::HandleCloseCallback);
}

UV_CLOSE_CB_FOR_CLASS(HandleScope, HandleClose) {
    num_handles_on_closing_--;
    if (num_handles_on_closing_ == 0 && handles_.empty()) {
        finish_callback_();
    }
}

Timer::Timer()
    : timerfd_(-1) {}

Timer::~Timer() {}

void Timer::Init(uv_loop_t* loop, std::function<void(Timer*)> callback) {
    timerfd_ = io_utils::CreateTimerFd();
    CHECK(timerfd_ != -1);
    UV_CHECK_OK(uv_poll_init(loop, &uv_handle_, timerfd_));
    uv_handle_.data = this;
    UV_CHECK_OK(uv_poll_start(&uv_handle_, UV_READABLE, &Timer::ExpiredCallback));
    cb_ = callback;
}

void Timer::Close() {
    if (timerfd_ != -1) {
        uv_close(UV_AS_HANDLE(&uv_handle_), nullptr);
    }
}

void Timer::ExpireIn(absl::Duration duration) {
    CHECK(timerfd_ != -1);
    CHECK(io_utils::SetupTimerFd(timerfd_, absl::ToInt64Microseconds(duration)));
}

void Timer::StochasticExpireIn(absl::Duration duration) {
    CHECK(timerfd_ != -1);
    double x = 1.0 - utils::GetRandomDouble(0.0, 1.0);  // x ends in (0, 1]
    double timeout_us = absl::ToDoubleMicroseconds(duration) * (-log(x));
    CHECK(io_utils::SetupTimerFd(timerfd_, gsl::narrow_cast<int>(timeout_us)));
}

void Timer::PeriodicExpire(absl::Duration interval) {
    CHECK(timerfd_ != -1);
    CHECK(io_utils::SetupTimerFd(timerfd_, 0, absl::ToInt64Microseconds(interval)));
}

UV_POLL_CB_FOR_CLASS(Timer, Expired) {
    cb_(this);
}

}  // namespace uv
}  // namespace faas
