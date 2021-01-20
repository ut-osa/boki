#include "utils/timerfd.h"

#include <sys/types.h>
#include <sys/timerfd.h>

namespace faas {
namespace io_utils {

int CreateTimerFd() {
    int fd = timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create timerfd";
        return -1;
    }
    return fd;
}

bool SetupTimerFdOneTime(int fd, absl::Duration duration) {
    struct itimerspec spec;
    memset(&spec, 0, sizeof(spec));
    spec.it_value = absl::ToTimespec(duration);
    if (timerfd_settime(fd, 0, &spec, nullptr) != 0) {
        PLOG(ERROR) << "timerfd_settime failed";
        return false;
    }
    return true;
}

bool SetupTimerFdPeriodic(int fd, absl::Duration initial, absl::Duration duration) {
    struct itimerspec spec;
    spec.it_value = absl::ToTimespec(initial);
    spec.it_interval = absl::ToTimespec(duration);
    if (timerfd_settime(fd, 0, &spec, nullptr) != 0) {
        PLOG(ERROR) << "timerfd_settime failed";
        return false;
    }
    return true;
}

}  // namespace io_utils
}  // namespace faas
