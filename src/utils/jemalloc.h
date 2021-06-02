#pragma once

#include "base/common.h"

#ifdef __FAAS_HAVE_JEMALLOC

#include <jemalloc/jemalloc.h>

namespace faas {
namespace jemalloc {

void PrintStat();

inline size_t GoodMallocSize(size_t min_size) {
    if (min_size == 0) {
        return 0;
    }
    size_t ret = nallocx(min_size, 0);
    return ret != 0 ? ret : min_size;
}

}  // namespace jemalloc
}  // namespace faas

#endif  // __FAAS_HAVE_JEMALLOC
