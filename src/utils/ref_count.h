#pragma once

#include "base/common.h"

namespace faas {

template<class T>
class RefCountedBase {
public:
    RefCountedBase()
        : ref_count_(1) {}

    virtual ~RefCountedBase() {
#if DCHECK_IS_ON()
        long value = ref_count_.load(std::memory_order_acq_rel);
        if (value != 0) {
            LOG(FATAL) << "The reference counter is not zero within destructor!";
        }
#endif
    }

    void Ref() {
        ref_count_.fetch_add(1, std::memory_order_relaxed);
    }

    void Unref() {
        long value = ref_count_.fetch_add(-1, std::memory_order_acq_rel);
        if (__FAAS_PREDICT_FALSE(value == 1)) {
            RefBecomesZero();
        }
#if DCHECK_IS_ON()
        if (__FAAS_PREDICT_FALSE(value <= 0)) {
            LOG(FATAL) << "The reference counter is less than zero!";
        }
#endif
    }

    class Accessor {
    public:
        Accessor() : target_(nullptr) {}
        ~Accessor() { Clear(); }

        void Clear() {
            if (target_ != nullptr) {
                target_->Unref();
                target_ = nullptr;
            }
        }

        T& operator*() const noexcept { return *DCHECK_NOTNULL(target_); }
        T* operator->() const noexcept { return DCHECK_NOTNULL(target_); }

        Accessor(const Accessor& other) noexcept
            : target_(other.target_) { if (target_ != nullptr) target_->Ref(); }
        Accessor(Accessor&& other) noexcept
            : target_(other.target_) { other.target_ = nullptr; }

        Accessor& operator=(const Accessor& other) noexcept {
            if (this == &other) {
                return *this;
            }
            Clear();
            target_ = other.target_;
            if (target_ != nullptr) {
                target_->Ref();
            }
            return *this;
        }

        Accessor& operator=(Accessor&& other) noexcept {
            if (this == &other) {
                return *this;
            }
            Clear();
            target_ = other.target_;
            other.target_ = nullptr;
            return *this;
        }

    private:
        friend class RefCountedBase;
        explicit Accessor(T* target) noexcept
            : target_(DCHECK_NOTNULL(target)) { target_->Ref(); }

        T* target_;
    };

    Accessor MakeAccessor() {
        return Accessor(static_cast<T*>(this));
    }

protected:
    virtual void RefBecomesZero() = 0;

private:
    std::atomic<long> ref_count_;

    DISALLOW_COPY_AND_ASSIGN(RefCountedBase);
};

}  // namespace faas
