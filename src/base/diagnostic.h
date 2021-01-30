#pragma once

#if defined(__FAAS_SRC) && !defined(__FAAS_NOWARN_CONVERSION)

#ifdef __clang__

#pragma clang diagnostic error "-Wconversion"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#ifdef __FAAS_NOWARN_SIGN_CONVERSION
#pragma clang diagnostic ignored "-Wsign-conversion"
#endif  // __FAAS_NOWARN_SIGN_CONVERSION

#define __BEGIN_THIRD_PARTY_HEADERS                       \
    _Pragma("clang diagnostic push")                      \
    _Pragma("clang diagnostic ignored \"-Wconversion\"")

#define __END_THIRD_PARTY_HEADERS                         \
    _Pragma("clang diagnostic pop")

#define __CLANG_CONVERSION_DIAGNOSTIC_ENABLED

#endif  // __clang__

#endif  // defined(__FAAS_SRC) && !defined(__FAAS_NOWARN_CONVERSION)

#ifndef __BEGIN_THIRD_PARTY_HEADERS
#define __BEGIN_THIRD_PARTY_HEADERS
#endif

#ifndef __END_THIRD_PARTY_HEADERS
#define __END_THIRD_PARTY_HEADERS
#endif
