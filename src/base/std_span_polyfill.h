#pragma once

#include <gsl/span>

namespace std {
using gsl::span;
}  // namespace std

#define STRING_TO_SPAN(STR_VAR) \
    std::span<const char>((STR_VAR).data(), (STR_VAR).length())
