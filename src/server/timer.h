#pragma once

#include "base/common.h"
#include "server/types.h"

namespace faas {
namespace server {

class Timer final : public server::ConnectionBase {
public:
    using Callback = TimerCallback;
    Timer(int timer_type, Callback cb);
    ~Timer();

    void SetPeriodic(absl::Time initial, absl::Duration interval);

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    bool TriggerIn(absl::Duration d);

private:
    enum State { kCreated, kIdle, kScheduled, kClosing, kClosed };

    bool periodic_;
    absl::Time initial_;
    absl::Duration interval_;

    Callback cb_;
    IOWorker* io_worker_;
    State state_;
    int timerfd_;

    DISALLOW_COPY_AND_ASSIGN(Timer);
};

}  // namespace server
}  // namespace faas
