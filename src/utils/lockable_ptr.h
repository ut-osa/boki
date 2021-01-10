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
    explicit LockablePtr(std::unique_ptr<T> target) {
        if (target != nullptr) {
            inner_.reset(new Inner);
            inner_->target = std::move(target);
        }
    }

    // LockablePtr is copyable, thus can be shared between threads
    LockablePtr(const LockablePtr& other) = default;
    LockablePtr(LockablePtr&& other) = default;

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

        T& operator*() const noexcept { return *target_; }
        T* operator->() const noexcept { return target_; }

        // Guard is movable
        Guard(Guard&& other) noexcept {
            mutex_ = other.mutex_;
            target_ = other.target_;
            other.mutex_ = nullptr;
        }
        Guard& operator=(Guard &&other) noexcept {
            mutex_ = other.mutex_;
            target_ = other.target_;
            other.mutex_ = nullptr;
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

        const T& operator*() const noexcept { return *target_; }
        const T* operator->() const noexcept { return target_; }

        // ReaderGuard is movable
        ReaderGuard(ReaderGuard&& other) noexcept {
            mutex_ = other.mutex_;
            target_ = other.target_;
            other.mutex_ = nullptr;
        }
        ReaderGuard& operator=(ReaderGuard &&other) noexcept {
            mutex_ = other.mutex_;
            target_ = other.target_;
            other.mutex_ = nullptr;
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

}  // namespace faas
