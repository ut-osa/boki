#pragma once

#define XXH_INLINE_ALL
#include <xxhash.h>

namespace faas {
namespace hash {

constexpr uint64_t kHashSeed64 = 0xecae064502f9bedcULL;

template<class IntType>
uint64_t xxHash64(IntType value) {
    return XXH64(&value, sizeof(IntType), kHashSeed64);
}

}  // namespace hash
}  // namespace faas
