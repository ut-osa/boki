#pragma once

#ifndef __FAAS_SRC
#error utils/lockable_ptr.h cannot be included outside
#endif

#include "base/common.h"

namespace faas {

template<class T>
class LockablePtr {
public:
    // LockablePtr takes ownership of target
    explicit LockablePtr(std::unique_ptr<T> target)
        : inner_(nullptr) {
        if (target != nullptr) {
            inner_.reset(new Inner);
            inner_->target = std::move(target);
        }
    }

    // LockablePtr is copyable, thus can be shared between threads
    LockablePtr(const LockablePtr& other) = default;
    LockablePtr(LockablePtr&& other) = default;

    // Check if holds a target object
    inline bool is_null() const noexcept { return inner_ == nullptr; }
    inline bool not_null() const noexcept { return inner_ != nullptr; }
    explicit operator bool() const noexcept { return not_null(); }

    class Guard {
    public:
        ~Guard() {
            if (mutex_ != nullptr) {
#if DCHECK_IS_ON()
                mutex_->AssertHeld();
#endif
                mutex_->Unlock();
            }
        }

        T& operator*() const noexcept { return *DCHECK_NOTNULL(target_); }
        T* operator->() const noexcept { return DCHECK_NOTNULL(target_); }

        // Guard is movable, but should avoid doing so explicitly
        Guard(Guard&& other) noexcept
            : mutex_(other.mutex_),
              target_(other.target_) {
            other.mutex_ = nullptr;
            other.target_ = nullptr;
        }
        Guard& operator=(Guard &&other) noexcept {
            if (this != &other) {
                mutex_ = other.mutex_;
                target_ = other.target_;
                other.mutex_ = nullptr;
                other.target_ = nullptr;
            }
            return *this;
        }

    private:
        friend class LockablePtr;
        absl::Mutex* mutex_;
        T* target_;

        Guard(absl::Mutex* mutex, T* target)
            : mutex_(mutex), target_(target) {}

        DISALLOW_COPY_AND_ASSIGN(Guard);
    };

    class ReaderGuard {
    public:
        ~ReaderGuard() {
            if (mutex_ != nullptr) {
#if DCHECK_IS_ON()
                mutex_->AssertReaderHeld();
#endif
                mutex_->ReaderUnlock();
            }
        }

        const T& operator*() const noexcept { return *DCHECK_NOTNULL(target_); }
        const T* operator->() const noexcept { return DCHECK_NOTNULL(target_); }

        // ReaderGuard is movable, but should avoid doing so explicitly
        ReaderGuard(ReaderGuard&& other) noexcept
            : mutex_(other.mutex_),
              target_(other.target_) {
            other.mutex_ = nullptr;
            other.target_ = nullptr;
        }
        ReaderGuard& operator=(ReaderGuard &&other) noexcept {
            if (this != &other) {
                mutex_ = other.mutex_;
                target_ = other.target_;
                other.mutex_ = nullptr;
                other.target_ = nullptr;
            }
            return *this;
        }

    private:
        friend class LockablePtr;
        absl::Mutex* mutex_;
        const T* target_;

        ReaderGuard(absl::Mutex* mutex, const T* target)
            : mutex_(mutex), target_(target) {}

        DISALLOW_COPY_AND_ASSIGN(ReaderGuard);
    };

    // Returned Guard must not live longer than parent LockablePtr
    Guard Lock() ABSL_NO_THREAD_SAFETY_ANALYSIS {
        if (__FAAS_PREDICT_FALSE(inner_ == nullptr)) {
            LOG(FATAL) << "Cannot Lock() on null pointer";
        }
#if DCHECK_IS_ON()
        inner_->mu.AssertNotHeld();
#endif
        inner_->mu.Lock();
        return Guard(&inner_->mu, inner_->target.get());
    }
    ReaderGuard ReaderLock() ABSL_NO_THREAD_SAFETY_ANALYSIS {
        if (__FAAS_PREDICT_FALSE(inner_ == nullptr)) {
            LOG(FATAL) << "Cannot ReaderLock() on null pointer";
        }
        inner_->mu.ReaderLock();
        return ReaderGuard(&inner_->mu, inner_->target.get());
    }

private:
    struct Inner {
        absl::Mutex        mu;
        std::unique_ptr<T> target;
    };
    std::shared_ptr<Inner> inner_;
};

template<class T>
bool operator==(const LockablePtr<T>& ptr, std::nullptr_t) noexcept {
    return ptr.is_null();
}

template<class T>
bool operator!=(const LockablePtr<T>& ptr, std::nullptr_t) noexcept {
    return ptr.not_null();
}

}  // namespace faas
