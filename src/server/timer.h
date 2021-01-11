#pragma once

#include "base/common.h"
#include "server/io_worker.h"

namespace faas {
namespace server {

class ServerBase;

class Timer final : public server::ConnectionBase {
public:
    typedef std::function<void()> Callback;
    Timer(int timer_type, Callback cb);
    ~Timer();

    void SetPeriodic(absl::Time initial, absl::Duration interval);

    void Start(IOWorker* io_worker) override;
    void ScheduleClose() override;

    bool TriggerIn(absl::Duration d);

private:
    friend class ServerBase;
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
