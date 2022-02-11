#include "utils/malloc.h"

#include "utils/format.h"

#ifdef __FAAS_USE_JEMALLOC

__BEGIN_THIRD_PARTY_HEADERS
#include <jemalloc/jemalloc.h>
__END_THIRD_PARTY_HEADERS

#define CTL_GET(n, v, t) do {              \
    size_t sz = sizeof(t);                 \
    mallctl(n, (void*)v, &sz, nullptr, 0); \
} while (0)

#endif  // __FAAS_USE_JEMALLOC

namespace faas {
namespace utils {

#ifdef __FAAS_USE_JEMALLOC

static constexpr size_t kAllocatedThresholdForProfDump = size_t{4}<<30; // 4GB

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

void ProfileDump(const char* file_path) {
    mallctl("prof.dump", nullptr, nullptr, (void*)&file_path, sizeof(const char*));
}

void WriteCallback(void* ptr, const char* s) {
    std::stringstream* stream = reinterpret_cast<std::stringstream*>(ptr);
    (*stream) << s;
}

std::string GetMallocStats(const char* opts) {
    std::stringstream stream;
    malloc_stats_print(&WriteCallback, &stream, opts);
    return stream.str();
}

std::atomic<size_t> prev_allocated{0};
}  // namespace

void PrintMallocStat() {
    if (!UpdateEpoch()) {
        LOG(WARNING) << "Failed to update epoch for jemalloc, will not print stat";
        return;
    }

    size_t allocated;
    CTL_GET("stats.allocated", &allocated, size_t);

    size_t old_allocated = prev_allocated.load(std::memory_order_relaxed);
    size_t delta = gsl::narrow_cast<size_t>(std::abs(
        gsl::narrow_cast<int64_t>(old_allocated) - gsl::narrow_cast<int64_t>(allocated)));
    if (delta < old_allocated / 20) {
        // The difference is belove 5%, will not print stats
        return;
    }
    prev_allocated.store(allocated);

    if (old_allocated == 0) {
        // First call of PrintStat, will invoke malloc_stats_print first
        LOG(INFO) << "jemalloc general information:\n"
                  << GetMallocStats("mdablxe");
    }

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

    if (allocated >= kAllocatedThresholdForProfDump) {
        bool prof_enabled;
        CTL_GET("opt.prof", &prof_enabled, bool);
        if (prof_enabled) {
            ProfileDump(nullptr);
        }
    }
}

size_t GoodMallocSize(size_t min_size) {
    if (min_size == 0) {
        return 0;
    }
    size_t ret = nallocx(min_size, 0);
    return ret != 0 ? ret : min_size;
}

#else  // __FAAS_USE_JEMALLOC

void PrintMallocStat() {}

size_t GoodMallocSize(size_t min_size) {
    return min_size;
}

#endif  // __FAAS_USE_JEMALLOC

}  // namespace utils
}  // namespace faas
