#pragma once

#include <gsl/span>

namespace std {
using gsl::span;
}  // namespace std

#define STRING_TO_SPAN(STR_VAR)                                   \
    std::span<const char>((STR_VAR).data(), (STR_VAR).length())

#define VECTOR_TO_CHAR_SPAN(VEC_VAR)                              \
    std::span<const char>(                                        \
        reinterpret_cast<const char*>((VEC_VAR).data()),          \
        sizeof(decltype(VEC_VAR)::value_type) * (VEC_VAR).size())
