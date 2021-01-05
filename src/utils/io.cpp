#define __FAAS_USED_IN_BINDING
#include "utils/io.h"

#include <sys/types.h>
#include <poll.h>
#include <fcntl.h>

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

}  // namespace io_utils
}  // namespace faas
