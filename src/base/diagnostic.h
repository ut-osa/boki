#pragma once

#ifdef __clang__

#ifndef __FAAS_NOWARN_CONVERSION

#pragma clang diagnostic error "-Wconversion"
#pragma clang diagnostic ignored "-Wimplicit-float-conversion"
#ifdef __FAAS_NOWARN_SIGN_CONVERSION
#pragma clang diagnostic ignored "-Wsign-conversion"
#endif  // __FAAS_NOWARN_SIGN_CONVERSION

#endif // __FAAS_NOWARN_CONVERSION

#define __BEGIN_THIRD_PARTY_HEADERS                       \
    _Pragma("clang diagnostic push")                      \
    _Pragma("clang diagnostic ignored \"-Wconversion\"")

#define __END_THIRD_PARTY_HEADERS                         \
    _Pragma("clang diagnostic pop")

#else  // __clang__

#define __BEGIN_THIRD_PARTY_HEADERS
#define __END_THIRD_PARTY_HEADERS

#endif  // __clang__
