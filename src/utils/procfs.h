#pragma once

#ifndef __FAAS_SRC
#error utils/protfs.h cannot be included outside
#endif

#include "base/common.h"

namespace faas {
namespace procfs_utils {

struct ThreadStat {
    int64_t timestamp;      // in ns
    int32_t cpu_stat_user;  // in tick, from /proc/self/task/[tid]/stat utime
    int32_t cpu_stat_sys;   // in tick, from /proc/self/task/[tid]/stat stime
    int32_t voluntary_ctxt_switches;     // from /proc/self/task/[tid]/status
    int32_t nonvoluntary_ctxt_switches;  // from /proc/self/task/[tid]/status
};

bool ReadThreadStat(int tid, ThreadStat* stat);

// Return contents of /proc/sys/kernel/hostname
std::string ReadHostname();

// Return sysconf(_SC_CLK_TCK)
int GetClockTicksPerSecond();

}  // namespace procfs_utils
}  // namespace faas
