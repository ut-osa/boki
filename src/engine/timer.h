#pragma once

#include "base/common.h"
#include "engine/io_worker.h"

namespace faas {
namespace engine {

class Timer final : public ConnectionBase {
public:
    typedef std::function<void()> Callback;
    Timer(int timer_type, Callback cb, int initial_duration_us);
    ~Timer();

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    bool TriggerIn(int duration_us);

private:
    enum State { kCreated, kIdle, kScheduled, kClosing, kClosed };

    Callback cb_;
    IOWorker* io_worker_;
    State state_;
    int timerfd_;
    int initial_duration_us_;

    DISALLOW_COPY_AND_ASSIGN(Timer);
};

}  // namespace engine
}  // namespace faas
