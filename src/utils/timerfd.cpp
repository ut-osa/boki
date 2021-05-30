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

uint64_t TimerFdRead(int fd) {
    uint64_t value;
    ssize_t nread = read(fd, &value, sizeof(uint64_t));
    if (nread < 0) {
        PLOG(ERROR) << "Failed to read timerfd";
    } else if (nread != sizeof(uint64_t)) {
        LOG(FATAL) << "Incorrect read size: " << nread;
    }
    return value;
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
