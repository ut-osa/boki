#pragma once

#include "base/common.h"
#include "engine/io_worker.h"

namespace faas {
namespace engine {

class Timer final : public ConnectionBase {
public:
    typedef std::function<void()> Callback;
    Timer(int timer_type, Callback cb);
    ~Timer();

    void SetPeriodic(absl::Time initial, absl::Duration duration);

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    bool TriggerIn(absl::Duration d);

private:
    enum State { kCreated, kIdle, kScheduled, kClosing, kClosed };

    bool periodic_;
    absl::Time initial_;
    absl::Duration duration_;

    Callback cb_;
    IOWorker* io_worker_;
    State state_;
    int timerfd_;

    DISALLOW_COPY_AND_ASSIGN(Timer);
};

}  // namespace engine
}  // namespace faas
