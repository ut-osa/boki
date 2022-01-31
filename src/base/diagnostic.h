#pragma once

#ifdef __FAAS_SRC

#ifdef __clang__

#pragma clang diagnostic error "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wunused-private-field"

#if !defined(__FAAS_NOWARN_CONVERSION)
#define __CLANG_CONVERSION_DIAGNOSTIC_ENABLED
#pragma clang diagnostic error "-Wconversion"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#endif  // !defined(__FAAS_NOWARN_CONVERSION)

#ifdef __FAAS_NOWARN_SIGN_CONVERSION
#pragma clang diagnostic ignored "-Wsign-conversion"
#endif  // __FAAS_NOWARN_SIGN_CONVERSION

#define __BEGIN_THIRD_PARTY_HEADERS                                          \
    _Pragma("clang diagnostic push")                                         \
    _Pragma("clang diagnostic ignored \"-Winconsistent-missing-override\"")  \
    _Pragma("clang diagnostic ignored \"-Wimplicit-fallthrough\"")           \
    _Pragma("clang diagnostic ignored \"-Wconversion\"")                     \
    _Pragma("clang diagnostic ignored \"-Winvalid-offsetof\"")

#define __END_THIRD_PARTY_HEADERS                                            \
    _Pragma("clang diagnostic pop")

#endif  // __clang__

#endif  // __FAAS_SRC

#ifndef __BEGIN_THIRD_PARTY_HEADERS
#define __BEGIN_THIRD_PARTY_HEADERS
#endif

#ifndef __END_THIRD_PARTY_HEADERS
#define __END_THIRD_PARTY_HEADERS
#endif
