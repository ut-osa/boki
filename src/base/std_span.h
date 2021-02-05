#pragma once

#include <gsl/span>
#include <gsl/span_ext>

namespace std {
using gsl::span;
}  // namespace std

#define EMPTY_CHAR_SPAN std::span<const char>()

#define STRING_AS_SPAN(STR_VAR)                                   \
    std::span<const char>((STR_VAR).data(), (STR_VAR).length())

#define VECTOR_AS_SPAN(VEC_VAR)                                   \
    std::span<const decltype(VEC_VAR)::value_type>(               \
        (VEC_VAR).data(), (VEC_VAR).size())

#define VECTOR_AS_CHAR_SPAN(VEC_VAR)                              \
    std::span<const char>(                                        \
        reinterpret_cast<const char*>((VEC_VAR).data()),          \
        sizeof(decltype(VEC_VAR)::value_type) * (VEC_VAR).size())
