#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

std::string Base64Encode(std::span<const char> data) {
    static constexpr const char* kBase64Chars =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
        "0123456789"
        "+/";
    static constexpr const char kTrailingChar = '=';

    size_t in_len = data.size();
    size_t len_encoded = (in_len + 2) / 3 * 4;
    std::string ret;
    ret.reserve(len_encoded);

    const unsigned char* bytes_to_encode = (const unsigned char*) data.data();
    size_t pos = 0;

    while (pos < in_len) {
        ret.push_back(kBase64Chars[(bytes_to_encode[pos + 0] & 0xfc) >> 2]);

        if (pos+1 < in_len) {
           ret.push_back(kBase64Chars[  ((bytes_to_encode[pos + 0] & 0x03) << 4)
                                      + ((bytes_to_encode[pos + 1] & 0xf0) >> 4)]);

           if (pos+2 < in_len) {
              ret.push_back(kBase64Chars[  ((bytes_to_encode[pos + 1] & 0x0f) << 2)
                                         + ((bytes_to_encode[pos + 2] & 0xc0) >> 6)]);
              ret.push_back(kBase64Chars[    bytes_to_encode[pos + 2] & 0x3f]);
           } else {
              ret.push_back(kBase64Chars[(bytes_to_encode[pos + 1] & 0x0f) << 2]);
              ret.push_back(kTrailingChar);
           }
        } else {
            ret.push_back(kBase64Chars[(bytes_to_encode[pos + 0] & 0x03) << 4]);
            ret.push_back(kTrailingChar);
            ret.push_back(kTrailingChar);
        }

        pos += 3;
    }

    return ret;
}

}  // namespace utils
}  // namespace faas
