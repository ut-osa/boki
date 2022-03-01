#include "base/thread.h"

#include "utils/env_variables.h"

#include <sched.h>
#include <errno.h>
#include <sys/syscall.h>

namespace faas {
namespace base {

namespace {

static int __gettid() {
    return gsl::narrow_cast<int>(syscall(SYS_gettid));
}

static constexpr size_t     kNumThreadSlots = 64;
static std::atomic<Thread*> thd_slots[kNumThreadSlots] = { nullptr };
static std::atomic<size_t>  next_thd_slot{0};

static void AddNewThread(Thread* thread) {
    size_t idx = next_thd_slot.fetch_add(1);
    if (idx >= kNumThreadSlots) {
        LOG(FATAL) << "Not enough statically allocated tid slots, "
                      "consider enlarge kNumThreadSlots";
    }
    thd_slots[idx].store(thread);
}

}  // namespace

thread_local Thread* Thread::current_{nullptr};

Thread::Thread(std::string_view name, std::function<void()> fn)
    : state_(kCreated),
      name_(std::string(name)),
      fn_(fn),
      tid_(-1) {}

Thread::~Thread() {
    if (name_ != kMainThreadName) {
        State state = state_.load();
        CHECK(state == kCreated || state == kFinished);
    }
}

void Thread::Start() {
    if (name_ == kMainThreadName) {
        LOG(FATAL) << "Cannot call Start() on the main thread";
    }
    if (!fn_) {
        LOG_F(FATAL, "Empty entry function for thread {}", name_);
    }
    state_.store(kStarting);
    CHECK_EQ(pthread_create(&pthread_, nullptr, &Thread::StartRoutine, this), 0);
    started_.WaitForNotification();
    CHECK(state_.load() == kRunning);
}

void Thread::Join() {
    if (name_ == kMainThreadName) {
        LOG(FATAL) << "Cannot call Join() on the main thread";
    }
    State state = state_.load();
    if (state == kFinished) {
        return;
    }
    CHECK(state == kRunning);
    CHECK_EQ(pthread_join(pthread_, nullptr), 0);
}

void Thread::Run() {
    tid_ = __gettid();
    state_.store(kRunning);
    AddNewThread(this);
    started_.Notify();
    LOG_F(INFO, "Start thread: {} (tid={})", name_, tid_);
    fn_();
    state_.store(kFinished);
}

void Thread::MarkThreadCategory(std::string_view category) {
    CHECK(current_ == this);
    // Set cpuset
    std::string cpuset_var_name(fmt::format("FAAS_{}_THREAD_CPUSET", category));
    std::string cpuset_str(utils::GetEnvVariable(cpuset_var_name));
    if (!cpuset_str.empty()) {
        cpu_set_t set;
        CPU_ZERO(&set);
        for (const std::string_view& cpu_str : absl::StrSplit(cpuset_str, ",")) {
            size_t cpu;
            CHECK(absl::SimpleAtoi(cpu_str, &cpu));
            CPU_SET(cpu, &set);
        }
        if (sched_setaffinity(0, sizeof(set), &set) != 0) {
            PLOG_F(FATAL, "Failed to set CPU affinity to {}", cpuset_str);
        } else {
            LOG_F(INFO, "Successfully set CPU affinity of current thread to {}", cpuset_str);
        }
    } else {
        LOG_F(INFO, "Does not find cpuset setting for {} threads (can be set by {})",
              category, cpuset_var_name);
    }
    // Set nice
    std::string nice_var_name(fmt::format("FAAS_{}_THREAD_NICE", category));
    std::string nice_str(utils::GetEnvVariable(nice_var_name));
    if (!nice_str.empty()) {
        int nice_value;
        CHECK(absl::SimpleAtoi(nice_str, &nice_value));
        int current_nice = nice(0);
        errno = 0;
        if (nice(nice_value - current_nice) == -1 && errno != 0) {
            PLOG_F(FATAL, "Failed to set nice to {}", nice_value);
        } else {
            CHECK_EQ(nice(0), nice_value);
            LOG_F(INFO, "Successfully set nice of current thread to {}", nice_value);
        }
    } else {
        LOG_F(INFO, "Does not find nice setting for {} threads (can be set by {})",
              category, nice_var_name);
    }
}

void* Thread::StartRoutine(void* arg) {
    Thread* self = reinterpret_cast<Thread*>(arg);
    current_ = self;
    self->Run();
    return nullptr;
}

namespace {
static Thread main_thread{Thread::kMainThreadName, nullptr};
}

void Thread::RegisterMainThread() {
    Thread* thread = &main_thread;
    thread->state_.store(kRunning);
    thread->tid_ = __gettid();
    thread->pthread_ = pthread_self();
    AddNewThread(thread);
    current_ = thread;
    LOG_F(INFO, "Register main thread: tid={}", thread->tid_);
}

std::vector<Thread*> Thread::GetAllThreads() {
    size_t n = next_thd_slot.load();
    std::vector<Thread*> threads;
    for (size_t i = 0; i < n; i++) {
        Thread* thread = thd_slots[i].load();
        if (thread != nullptr) {
            threads.push_back(thread);
        }
    }
    return threads;
}

}  // namespace base
}  // namespace faas
