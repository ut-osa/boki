#pragma once

#include "base/common.h"

namespace faas {
namespace io_utils {

int CreateTimerFd();

bool SetupTimerFdOneTime(int fd, absl::Duration duration);
bool SetupTimerFdPeriodic(int fd, absl::Duration initial, absl::Duration duration);

}  // namespace io_utils
}  // namespace faas
