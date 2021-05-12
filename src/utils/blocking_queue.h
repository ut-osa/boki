#pragma once

#ifndef __FAAS_SRC
#error utils/blocking_queue.h cannot be included outside
#endif

#include "base/common.h"

namespace faas {
namespace utils {

template<class T>
class BlockingQueue {
public:
    // A straightforward BlockingQueue implemented with mutex and condition variable
    BlockingQueue();
    ~BlockingQueue();

    void Stop();

    void Push(const T& value);
    void Push(T&& value);

    bool Pop(T* value);

private:
    absl::Mutex   mu_;
    absl::CondVar cv_;

    std::deque<T> queue_   ABSL_GUARDED_BY(mu_);
    bool          stopped_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(BlockingQueue);
};

// Start implementation of BlockingQueue

template<class T>
BlockingQueue<T>::BlockingQueue()
    : stopped_(false) {}

template<class T>
BlockingQueue<T>::~BlockingQueue() {
    DCHECK(stopped_);
    if (!queue_.empty()) {
        LOG_F(WARNING, "There are {} elements left in the queue", queue_.size());
    }
}

template<class T>
void BlockingQueue<T>::Stop() {
    absl::MutexLock lk(&mu_);
    stopped_ = true;
    cv_.SignalAll();
}

template<class T>
void BlockingQueue<T>::Push(const T& value) {
    absl::MutexLock lk(&mu_);
    if (stopped_) {
        LOG(INFO) << "This queue has stopped, will ignore this push";
        return;
    }
    queue_.push_back(value);
    cv_.Signal();
}

template<class T>
void BlockingQueue<T>::Push(T&& value) {
    absl::MutexLock lk(&mu_);
    if (stopped_) {
        LOG(INFO) << "This queue has stopped, will ignore this push";
        return;
    }
    queue_.push_back(value);
    cv_.Signal();
}

template<class T>
bool BlockingQueue<T>::Pop(T* value) {
    absl::MutexLock lk(&mu_);
    while (queue_.empty() && !stopped_) {
        cv_.Wait(&mu_);
    }
    if (stopped_) {
        return false;
    }
    DCHECK(!queue_.empty());
    *value = std::move(queue_.front());
    queue_.pop_front();
    return true;
}

}  // namespace utils
}  // namespace faas
