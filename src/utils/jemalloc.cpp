#include "utils/jemalloc.h"

#include <jemalloc/jemalloc.h>

#define CTL_GET(n, v, t) do {              \
    size_t sz = sizeof(t);                 \
    mallctl(n, (void *)v, &sz, NULL, 0);  \
} while (0)

namespace faas {
namespace jemalloc {

namespace {
bool UpdateEpoch() {
    uint64_t epoch = 1;
    size_t u64sz = sizeof(uint64_t);
    int err = mallctl("epoch", (void*)&epoch, &u64sz, (void*)&epoch, sizeof(uint64_t));
    if (err != 0) {
        if (err == EAGAIN) {
            return false;
        }
        LOG(FATAL) << "<jemalloc>: Failure in mallctl(\"epoch\", ...)";
    }
    return true;
}

std::string FormatBytes(size_t bytes) {
    if (bytes < size_t{10} * 1024) {
        return fmt::format("{}B", bytes);
    }
    if (bytes < size_t{10} * 1024 * 1024) {
        return fmt::format("{:.1f}KB", static_cast<float>(bytes)/1024);
    }
    if (bytes < size_t{10} * 1024 * 1024 * 1024) {
        return fmt::format("{:.1f}MB", static_cast<float>(bytes)/1024/1024);
    }
    return fmt::format("{:.1f}GB", static_cast<float>(bytes)/1024/1024/1024);
}

std::atomic<size_t> prev_allocated{0};
}  // namespace

void PrintStat() {
    if (!UpdateEpoch()) {
        LOG(WARNING) << "Failed to update epoch for jemalloc, will not print stat";
        return;
    }

    size_t allocated;
    CTL_GET("stats.allocated", &allocated, size_t);

    static constexpr int64_t kAllocatedThreshold = 1024;
    int64_t old_allocated = gsl::narrow_cast<int64_t>(
        prev_allocated.load(std::memory_order_relaxed));
    if (std::abs(old_allocated - gsl::narrow_cast<int64_t>(allocated)) < kAllocatedThreshold) {
        // The difference is too tiny, will not print stats
        return;
    }
    prev_allocated.store(allocated);

    size_t resident;
    size_t mapped;
    size_t metadata;
    size_t retained;    
    CTL_GET("stats.resident", &resident, size_t);
    CTL_GET("stats.mapped",   &mapped,   size_t);
    CTL_GET("stats.metadata", &metadata, size_t);
    CTL_GET("stats.retained", &retained, size_t);

    LOG(INFO) << "[STAT] jemalloc: "
              << "allocated=" << FormatBytes(allocated) << ", "
              << "resident="  << FormatBytes(resident)  << ", "
              << "mapped="    << FormatBytes(mapped)    << ", "
              << "metadata="  << FormatBytes(metadata)  << ", "
              << "retained="  << FormatBytes(retained);
}

}  // namespace jemalloc
}  // namespace faas
