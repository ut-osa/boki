#pragma once

#ifndef __FAAS_SRC
#error base/thread.h cannot be included outside
#endif

#include "base/common.h"
#include <pthread.h>

namespace faas {
namespace base {

class Thread {
public:
    static constexpr const char* kMainThreadName = "Main";

    Thread(std::string_view name, std::function<void()> fn);
    ~Thread();

    void Start();
    void Join();

    void MarkThreadCategory(std::string_view category);

    const char* name() const { return name_.c_str(); }
    int tid() const { return tid_; }

    static Thread* current() { return DCHECK_NOTNULL(current_); }

    static void RegisterMainThread();

private:
    enum State { kCreated, kStarting, kRunning, kFinished };

    std::atomic<State> state_;
    std::string name_;
    std::function<void()> fn_;
    int tid_;

    absl::Notification started_;
    pthread_t pthread_;

    static thread_local Thread* current_;

    void Run();
    static void* StartRoutine(void* arg);

    DISALLOW_COPY_AND_ASSIGN(Thread);
};

}  // namespace base
}  // namespace faas
