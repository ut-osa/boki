#pragma once

#define XXH_INLINE_ALL
#include <xxhash.h>

namespace faas {
namespace hash {

constexpr uint64_t kDefaultHashSeed64 = 0xecae064502f9bedcULL;

template<class IntType>
uint64_t xxHash64(IntType value, uint64_t seed = kDefaultHashSeed64) {
    return XXH64(&value, sizeof(IntType), seed);
}

}  // namespace hash
}  // namespace faas
