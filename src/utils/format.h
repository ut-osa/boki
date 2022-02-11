#pragma once

#include "base/common.h"
#include "utils/float.h"

namespace faas {
namespace utils {

inline std::string FormatBytes(size_t bytes) {
    if (bytes < size_t{10} * 1024) {
        return fmt::format("{}B", bytes);
    }
    if (bytes < size_t{10} * 1024 * 1024) {
        return fmt::format("{:.1f}KB", float_utils::GetRatio<float>(bytes, 1<<10));
    }
    if (bytes < size_t{10} * 1024 * 1024 * 1024) {
        return fmt::format("{:.1f}MB", float_utils::GetRatio<float>(bytes, 1<<20));
    }
    return fmt::format("{:.1f}GB", float_utils::GetRatio<float>(bytes, 1<<30));
}

inline std::string FormatNumber(size_t number) {
    if (number < size_t{10} * 1000) {
        return fmt::format("{}", number);
    }
    if (number < size_t{10} * 1000 * 1000) {
        return fmt::format("{:.1f}K", float_utils::AsFloat(number) / 1e3f);
    }
    if (number < size_t{10} * 1000 * 1000 * 1000) {
        return fmt::format("{:.1f}M", float_utils::AsFloat(number) / 1e6f);
    }
    return fmt::format("{:.1f}B", float_utils::AsFloat(number) / 1e9f);
}

}  // namespace utils
}  // namespace faas
