#pragma once

#include "base/common.h"
#include "utils/malloc.h"

#ifdef __FAAS_HAVE_ABSL
__BEGIN_THIRD_PARTY_HEADERS
#include <absl/container/inlined_vector.h>
#include <absl/synchronization/mutex.h>
__END_THIRD_PARTY_HEADERS
#endif

#ifdef __FAAS_SRC
__BEGIN_THIRD_PARTY_HEADERS
#include <google/protobuf/arena.h>
__END_THIRD_PARTY_HEADERS
#endif

namespace faas {
namespace utils {

// SimpleObjectPool is NOT thread-safe
template<class T>
class SimpleObjectPool {
public:
    SimpleObjectPool() {
        total_capacity_ = 0;
        AllocBlock(/* min_capacity= */ 1);
    }

    ~SimpleObjectPool() {
        for (const auto& block : blocks_) {
            FreeBlock(block);
        }
    }

    T* Get() {
        if (!free_objs_.empty()) {
            T* obj = free_objs_.back();
            free_objs_.pop_back();
            return obj;
        }
        return GetSlowPath();
    }

    void Return(T* obj) {
        free_objs_.push_back(obj);
    }

private:
#ifdef __FAAS_HAVE_ABSL
    absl::InlinedVector<T*, 15> free_objs_;
#else
    std::vector<T*> free_objs_;
#endif

    struct Block {
        char*  base;
        size_t malloc_size;
        size_t capacity;
        size_t used;
    };
    std::vector<Block> blocks_;
    size_t total_capacity_;

    inline static T* ObjectPtr(const Block& block, size_t idx) {
        DCHECK_LT(idx, block.capacity);
        T* obj = reinterpret_cast<T*>(block.base + sizeof(T) * idx);
        return obj;
    }

    inline static T* NextObject(Block& block) {
        DCHECK_LT(block.used, block.capacity);
        T* obj = ObjectPtr(block, block.used++);
        new(obj) T();
        return obj;
    }

    T* GetNewBlockPath() {
        size_t delta_capacity = total_capacity_;
        AllocBlock(delta_capacity);
        return NextObject(blocks_.back());
    }

    T* GetSlowPath() {
        Block& block = blocks_.back();
        if (block.used < block.capacity) {
            return NextObject(block);
        } else {
            return GetNewBlockPath();
        }
    }

    void AllocBlock(size_t min_capacity) {
        size_t size = std::max<size_t>(__FAAS_PAGE_SIZE, min_capacity * sizeof(T));
        size = GoodMallocSize(size);
        VLOG_F(1, "Allocate pool block: size={}, capacity={}", size, size / sizeof(T));
        blocks_.push_back(Block {
            .base        = reinterpret_cast<char*>(DCHECK_NOTNULL(malloc(size))),
            .malloc_size = size,
            .capacity    = size / sizeof(T),
            .used        = 0,
        });
        total_capacity_ += size / sizeof(T);
    }

    void FreeBlock(const Block& block) {
        for (size_t i = 0; i < block.used; i++) {
            ObjectPtr(block, i)->~T();
        }
        free(block.base);
    }

    DISALLOW_COPY_AND_ASSIGN(SimpleObjectPool);
};

#ifdef __FAAS_SRC

template<class T>
class ProtobufMessagePool {
public:
    ProtobufMessagePool() {}
    ~ProtobufMessagePool() {}

    T* Get() {
        if (free_objs_.empty()) {
            free_objs_.push_back(google::protobuf::Arena::CreateMessage<T>(&arena_));
        }
        T* obj = free_objs_.back();
        free_objs_.pop_back();
        return obj; 
    }

    void Return(T* obj) {
        free_objs_.push_back(obj);
    }

private:
    google::protobuf::Arena arena_;
    absl::InlinedVector<T*, 16> free_objs_;

    DISALLOW_COPY_AND_ASSIGN(ProtobufMessagePool);
};

template<class T>
class ThreadSafeObjectPool {
public:
    ThreadSafeObjectPool() {}
    ~ThreadSafeObjectPool() {}

    T* Get() {
        T* obj = nullptr;
        {
            absl::MutexLock lk(&mu_);
            obj = inner_.Get();
        }
        return DCHECK_NOTNULL(obj);
    }

    void Return(T* obj) {
        absl::MutexLock lk(&mu_);
        inner_.Return(obj);
    }

private:
    absl::Mutex mu_;
    SimpleObjectPool<T> inner_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadSafeObjectPool);
};

#endif  // __FAAS_SRC

}  // namespace utils
}  // namespace faas
