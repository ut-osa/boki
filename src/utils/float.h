#pragma once

#include "base/common.h"

namespace faas {
namespace float_utils {

template<class T>
float AsFloat(T value) {
    static_assert(std::is_integral<T>::value || std::is_floating_point<T>::value);
    return static_cast<float>(value);
}

template<class T>
double AsDouble(T value) {
    static_assert(std::is_integral<T>::value || std::is_floating_point<T>::value);
    return static_cast<double>(value);
}

template<class FloatT, class LIntT, class RIntT>
FloatT GetRatio(LIntT lhs, RIntT rhs) {
    static_assert(std::is_floating_point<FloatT>::value);
    static_assert(std::is_integral<LIntT>::value);
    static_assert(std::is_integral<RIntT>::value);
    return static_cast<FloatT>(lhs) / static_cast<FloatT>(rhs);
}

}  // namespace float_utils
}  // namespace faas
