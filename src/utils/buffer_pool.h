#pragma once

#ifndef __FAAS_SRC
#error utils/buffer_pool.h cannot be included outside
#endif

#include "base/common.h"

namespace faas {
namespace utils {

// BufferPool is NOT thread-safe
class BufferPool {
public:
    BufferPool(std::string_view pool_name, size_t buffer_size)
        : pool_name_(std::string(pool_name)), buffer_size_(buffer_size) {}
    ~BufferPool() = default;

    size_t buffer_size() const { return buffer_size_; }

    void Get(char** buf, size_t* size) {
        if (available_buffers_.empty()) {
            std::unique_ptr<char[]> new_buffer(new char[buffer_size_]);
            available_buffers_.push_back(new_buffer.get());
            all_buffers_.push_back(std::move(new_buffer));
            LOG(INFO) << "BufferPool[" << pool_name_ << "]: Allocate new buffer, "
                      << "current buffer count is " << all_buffers_.size();
        }
        *buf = available_buffers_.back();
        available_buffers_.pop_back();
        *size = buffer_size_;
    }

    void Get(std::span<char>* buf) {
        char* base;
        size_t len;
        Get(&base, &len);
        *buf = std::span<char>(base, len);
    }

    void Return(char* buf) {
        available_buffers_.push_back(buf);
    }

    void Return(std::span<char> buf) {
        DCHECK_EQ(buf.size(), buffer_size_);
        Return(buf.data());
    }

private:
    std::string pool_name_;
    size_t buffer_size_;
    absl::InlinedVector<char*, 16> available_buffers_;
    absl::InlinedVector<std::unique_ptr<char[]>, 16> all_buffers_;

    DISALLOW_COPY_AND_ASSIGN(BufferPool);
};

}  // namespace utils
}  // namespace faas
