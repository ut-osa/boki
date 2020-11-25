#pragma once

#include "base/common.h"

#ifdef __FAAS_HAVE_ABSL
#include <absl/container/flat_hash_map.h>
#include <absl/container/inlined_vector.h>
#include <absl/synchronization/mutex.h>
#include <absl/synchronization/notification.h>
#endif

#ifdef __FAAS_SRC
#include "base/thread.h"
#include <google/protobuf/arena.h>
#endif

namespace faas {
namespace utils {

template<class T>
T* DefaultObjectConstructor() {
    return new T();
}

// SimpleObjectPool is NOT thread-safe
template<class T>
class SimpleObjectPool {
public:
    explicit SimpleObjectPool(std::function<T*()> object_constructor = DefaultObjectConstructor<T>)
        : object_constructor_(object_constructor) {}

    ~SimpleObjectPool() {}

    T* Get() {
        if (free_objs_.empty()) {
            T* new_obj = object_constructor_();
            free_objs_.push_back(new_obj);
            objs_.emplace_back(new_obj);
        }
        DCHECK(!free_objs_.empty());
        T* obj = free_objs_.back();
        free_objs_.pop_back();
        return obj;
    }

    void Return(T* obj) {
        free_objs_.push_back(obj);
    }

private:
    std::function<T*()> object_constructor_;
#ifdef __FAAS_HAVE_ABSL
    absl::InlinedVector<std::unique_ptr<T>, 16> objs_;
    absl::InlinedVector<T*, 16> free_objs_;
#else
    std::vector<std::unique_ptr<T>> objs_;
    std::vector<T*> free_objs_;
#endif

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
    explicit ThreadSafeObjectPool(std::function<T*()> object_constructor = DefaultObjectConstructor<T>)
        : object_constructor_(object_constructor) {}

    ~ThreadSafeObjectPool() {}

    T* Get() {
        auto free_objs = base::Thread::current()->LocalStorageGet<std::vector<T*>>(this);
        if (free_objs->empty()) {
            T* new_obj = object_constructor_();
            free_objs->push_back(new_obj);
            absl::MutexLock lk(&mu_);
            objs_.emplace_back(new_obj);
        }
        DCHECK(!free_objs->empty());
        T* obj = free_objs->back();
        free_objs->pop_back();
        return obj;
    }

    void Return(T* obj) {
        auto free_objs = base::Thread::current()->LocalStorageGet<std::vector<T*>>(this);
        free_objs->push_back(obj);
    }

private:
    std::function<T*()> object_constructor_;

    absl::Mutex mu_;
    absl::InlinedVector<std::unique_ptr<T>, 16> objs_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadSafeObjectPool);
};

#endif  // __FAAS_SRC

}  // namespace utils
}  // namespace faas
