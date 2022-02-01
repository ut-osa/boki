#pragma once

#if defined(__FAAS_SRC) && !defined(__INTELLISENSE__)

#if defined(__clang__)
// Compiler is Clang

#pragma clang diagnostic error "-Wimplicit-fallthrough"
#pragma clang diagnostic ignored "-Wunused-private-field"

#if !defined(__FAAS_NOWARN_CONVERSION)
#define __CLANG_CONVERSION_DIAGNOSTIC_ENABLED
#pragma clang diagnostic error "-Wconversion"
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

#elif defined(__GNUC__)
// Compiler is GCC

#if !defined(__FAAS_NOWARN_CONVERSION)
#define __GCC_CONVERSION_DIAGNOSTIC_ENABLED
#pragma GCC diagnostic error "-Wconversion"
#pragma GCC diagnostic error "-Wsign-conversion"
#pragma GCC diagnostic ignored "-Wfloat-conversion"
#endif  // !defined(__FAAS_NOWARN_CONVERSION)

#ifdef __FAAS_NOWARN_SIGN_CONVERSION
#pragma GCC diagnostic ignored "-Wsign-conversion"
#endif  // __FAAS_NOWARN_SIGN_CONVERSION

#define __BEGIN_THIRD_PARTY_HEADERS                                          \
    _Pragma("GCC diagnostic push")                                           \
    _Pragma("GCC diagnostic ignored \"-Wconversion\"")                       \
    _Pragma("GCC diagnostic ignored \"-Wsign-conversion\"")

#define __END_THIRD_PARTY_HEADERS                                            \
    _Pragma("GCC diagnostic pop")

#endif

#endif  // defined(__FAAS_SRC) && !defined(__INTELLISENSE__)

#ifndef __BEGIN_THIRD_PARTY_HEADERS
#define __BEGIN_THIRD_PARTY_HEADERS
#endif

#ifndef __END_THIRD_PARTY_HEADERS
#define __END_THIRD_PARTY_HEADERS
#endif
