#include "utils/io.h"

#include <sys/types.h>
#include <poll.h>
#include <fcntl.h>
#include <sys/timerfd.h>

namespace faas {
namespace io_utils {

void FdSetNonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    PCHECK(flags != -1) << "fcntl F_GETFL failed";
    PCHECK(fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0)
        << "fcntl F_SETFL failed";
}

void FdUnsetNonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    PCHECK(flags != -1) << "fcntl F_GETFL failed";
    PCHECK(fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) == 0)
        << "fcntl F_SETFL failed";
}

bool FdPollForRead(int fd, int timeout_ms) {
    struct pollfd pfd;
    pfd.fd = fd;
    pfd.events = POLLIN;
    int ret = poll(&pfd, 1, timeout_ms);
    if (ret == -1) {
        PLOG(ERROR) << "poll failed";
        return false;
    }
    if (ret == 0) {
        LOG(ERROR) << "poll on given fifo timeout";
        return false;
    }
    if ((pfd.revents & POLLIN) == 0) {
        LOG(ERROR) << "Error happens on given fifo: revents=" << pfd.revents;
        return false;
    }
    return true;
}

int CreateTimerFd() {
    int fd = timerfd_create(CLOCK_MONOTONIC, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create timerfd";
        return -1;
    }
    return fd;
}

bool SetupTimerFd(int fd, int initial_us, int interval_us) {
    VLOG(2) << fmt::format("SetupTimerFd: initial_us={}, interval_us={}",
                           initial_us, interval_us);
    struct itimerspec spec;
    memset(&spec, 0, sizeof(spec));
    if (initial_us == 0) {
        spec.it_value.tv_nsec = 1;
    } else {
        spec.it_value.tv_sec = initial_us / 1000000;
        spec.it_value.tv_nsec = initial_us % 1000000 * 1000;
    }
    spec.it_interval.tv_sec = interval_us / 1000000;
    spec.it_interval.tv_nsec = interval_us % 1000000 * 1000;
    if (timerfd_settime(fd, 0, &spec, nullptr) != 0) {
        PLOG(ERROR) << "timerfd_settime failed";
        return false;
    }
    return true;
}

}  // namespace io_utils
}  // namespace faas
