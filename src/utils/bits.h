#pragma once

#include "base/common.h"

namespace faas {
namespace bits {

inline uint64_t JoinTwo32(uint32_t high, uint32_t low) {
    return uint64_t{low} + (uint64_t{high} << 32);
}

inline uint32_t JoinTwo16(uint16_t high, uint16_t low) {
    return uint32_t{low} + (uint32_t{high} << 16);
}

inline uint32_t LowHalf64(uint64_t x) {
    return gsl::narrow_cast<uint32_t>(x);
}

inline uint16_t LowHalf32(uint32_t x) {
    return gsl::narrow_cast<uint16_t>(x);
}

inline uint32_t HighHalf64(uint64_t x) {
    return gsl::narrow_cast<uint32_t>(x >> 32);
}

inline uint16_t HighHalf32(uint32_t x) {
    return gsl::narrow_cast<uint16_t>(x >> 16);
}

inline std::string HexStr(uint64_t x) {
    return fmt::format("{:016x}", x);
}

inline std::string HexStr(uint32_t x) {
    return fmt::format("{:08x}", x);
}

inline std::string HexStr(uint16_t x) {
    return fmt::format("{:04x}", x);
}

inline std::string HexStr0x(uint64_t x) {
    return fmt::format("{:#018x}", x);
}

inline std::string HexStr0x(uint32_t x) {
    return fmt::format("{:#010x}", x);
}

inline std::string HexStr0x(uint16_t x) {
    return fmt::format("{:#06x}", x);
}

}  // namespace bits
}  // namespace faas
