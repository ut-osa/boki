#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

void PrintMallocStat(std::string header);

size_t GoodMallocSize(size_t min_size);

}  // namespace utils
}  // namespace faas
