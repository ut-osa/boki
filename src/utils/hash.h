#pragma once

#define XXH_INLINE_ALL
__BEGIN_THIRD_PARTY_HEADERS
#include <xxhash.h>
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace hash {

constexpr uint64_t kDefaultHashSeed64 = 0xecae064502f9bedcULL;

template<class IntType>
uint64_t xxHash64(IntType value, uint64_t seed = kDefaultHashSeed64) {
    return XXH64(&value, sizeof(IntType), seed);
}

inline uint64_t xxHash64(std::span<const char> data,
                         uint64_t seed = kDefaultHashSeed64) {
    return XXH64(data.data(), data.size(), seed);
}

inline uint64_t xxHash64(std::initializer_list<std::span<const char>> data_vec,
                         uint64_t seed = kDefaultHashSeed64) {
    XXH64_state_t* state = XXH64_createState();
    CHECK_EQ(XXH64_reset(state, seed), XXH_OK);
    for (const auto& data : data_vec) {
        CHECK_EQ(XXH64_update(state, data.data(), data.size()), XXH_OK);
    }
    uint64_t result = XXH64_digest(state);
    XXH64_freeState(state);
    return result;
}

}  // namespace hash
}  // namespace faas
