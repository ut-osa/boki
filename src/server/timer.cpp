#include "server/timer.h"

#include "utils/io.h"
#include "utils/timerfd.h"
#include "server/constants.h"

namespace faas {
namespace server {

Timer::Timer(int timer_type, Callback cb)
    : ConnectionBase(timer_type),
      periodic_(false),
      cb_(cb),
      io_worker_(nullptr),
      state_(kCreated),
      timerfd_(-1) {}

Timer::~Timer() {
    DCHECK(state_ == kCreated || state_ == kClosed);
    DCHECK(timerfd_ == -1);
}

void Timer::SetPeriodic(absl::Time initial, absl::Duration interval) {
    DCHECK(!periodic_ && state_ == kCreated);
    periodic_ = true;
    initial_ = initial;
    interval_ = interval;
}

void Timer::Start(server::IOWorker* io_worker) {
    DCHECK(io_worker->WithinMyEventLoopThread());
    io_worker_ = io_worker;
    timerfd_ = io_utils::CreateTimerFd();
    CHECK(timerfd_ != -1);
    io_utils::FdUnsetNonblocking(timerfd_);
    URING_DCHECK_OK(current_io_uring()->RegisterFd(timerfd_));
    state_ = kIdle;
    if (periodic_) {
        absl::Duration initial_duration = initial_ - absl::Now();
        if (initial_duration < absl::ZeroDuration()) {
            LOG(WARNING) << "Has past the initial duration";
            initial_duration = absl::Microseconds(1);
        }
        CHECK(io_utils::SetupTimerFdPeriodic(timerfd_, initial_duration, interval_));
        state_ = kScheduled;
    }
    URING_DCHECK_OK(current_io_uring()->StartRead(
        timerfd_, kOctaBufGroup,
        [this] (int status, std::span<const char> data) -> bool {
            if (state_ != kScheduled) {
                return false;
            }
            if (!periodic_) {
                state_ = kIdle;
            }
            cb_();
            return true;
        }
    ));
}

void Timer::ScheduleClose() {
    DCHECK(io_worker_->WithinMyEventLoopThread());
    state_ = kClosing;
    URING_DCHECK_OK(current_io_uring()->Close(timerfd_, [this] () {
        timerfd_ = -1;
        state_ = kClosed;
        io_worker_->OnConnectionClose(this);
    }));
}

bool Timer::TriggerIn(absl::Duration d) {
    DCHECK(!periodic_);
    DCHECK(io_worker_->WithinMyEventLoopThread());
    if (state_ != kIdle) {
        LOG(WARNING) << "Not in idle state, cannot schedule trigger of this timer!";
        return false;
    }
    state_ = kScheduled;
    CHECK(io_utils::SetupTimerFdOneTime(timerfd_, d));
    return true;
}

}  // namespace server
}  // namespace faas
