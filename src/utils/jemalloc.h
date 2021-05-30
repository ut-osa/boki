#pragma once

#ifndef __FAAS_SRC
#error utils/jemalloc.h cannot be included outside
#endif

#include "base/common.h"

namespace faas {
namespace jemalloc {

void PrintStat();

}  // namespace jemalloc
}  // namespace faas
