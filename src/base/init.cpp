#include "base/diagnostic.h"
#include "base/init.h"
#include "base/logging.h"
#include "base/thread.h"
#include "utils/docker.h"

#include <stdio.h>
#include <string.h>
#include <signal.h>

__BEGIN_THIRD_PARTY_HEADERS

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/debugging/symbolize.h>
#include <absl/debugging/failure_signal_handler.h>

__END_THIRD_PARTY_HEADERS

ABSL_FLAG(int, v, 0, "Show all VLOG(m) messages for m <= this.");
ABSL_FLAG(std::string, log_path, "",
          "Set file path for logging. If not set, stderr is used.");
ABSL_FLAG(bool, alsologtostderr, false,
          "If set, will log to stderr in addition to log_path");

#define RAW_CHECK(EXPR, MSG)             \
    do {                                 \
        if (!(EXPR)) {                   \
            fprintf(stderr, MSG "\n");   \
            exit(EXIT_FAILURE);          \
        }                                \
    } while (0)

namespace faas {
namespace base {

namespace {
static constexpr size_t      kNumCleanupFns = 64;
static std::function<void()> cleanup_fns[kNumCleanupFns];
static std::atomic<size_t>   next_cleanup_fn{0};

static std::function<void()> sigint_handler;

static void RaiseToDefaultHandler(int signo) {
    signal(signo, SIG_DFL);
    raise(signo);
}

static void SignalHandler(int signo) {
    if (signo == SIGTERM || signo == SIGABRT) {
        int n = static_cast<int>(next_cleanup_fn.load());
        if (n > 0) {
            fprintf(stderr, "Invoke %d clean-up functions\n", n);
            for (int i = n - 1; i >= 0; i--) {
                cleanup_fns[i]();
            }
        }
        fprintf(stderr, "Exit with failure\n");
        exit(EXIT_FAILURE);
    } else if (signo == SIGINT) {
        if (sigint_handler) {
            sigint_handler();
        } else {
            RaiseToDefaultHandler(SIGINT);
        }
    } else {
        RaiseToDefaultHandler(signo);
    }
}
}  // namespace

void InitMain(int argc, char* argv[],
              std::vector<char*>* positional_args) {
    absl::InitializeSymbolizer(argv[0]);

    struct sigaction act;
    memset(&act, 0, sizeof(struct sigaction));
    act.sa_handler = SignalHandler;
    RAW_CHECK(sigaction(SIGABRT, &act, nullptr) == 0,
              "Failed to set SIGABRT handler");
    RAW_CHECK(sigaction(SIGTERM, &act, nullptr) == 0,
              "Failed to set SIGTERM handler");
    RAW_CHECK(sigaction(SIGINT, &act, nullptr) == 0,
              "Failed to set SIGINT handler");

    absl::FailureSignalHandlerOptions options;
    options.call_previous_handler = true;
    absl::InstallFailureSignalHandler(options);

    std::vector<char*> unparsed_args = absl::ParseCommandLine(argc, argv);
    std::string log_path = absl::GetFlag(FLAGS_log_path);
    if (!log_path.empty()) {
        std::string container_id = docker_utils::GetSelfContainerShortId();
        if (container_id != docker_utils::kInvalidContainerShortId) {
            log_path = fmt::format("{}.{}", log_path, container_id);
        }
        logging::Init(absl::GetFlag(FLAGS_v),
                      log_path.c_str(),
                      absl::GetFlag(FLAGS_alsologtostderr));
    } else {
        logging::Init(absl::GetFlag(FLAGS_v));
    }

    if (positional_args == nullptr && unparsed_args.size() > 1) {
        LOG(FATAL) << "This program does not accept positional arguments";
    }
    if (positional_args != nullptr) {
        positional_args->clear();
        for (size_t i = 1; i < unparsed_args.size(); i++) {
            positional_args->push_back(unparsed_args[i]);
        }
    }

    Thread::RegisterMainThread();
}

void ChainCleanupFn(std::function<void()> fn) {
    size_t idx = next_cleanup_fn.fetch_add(1);
    if (idx >= kNumCleanupFns) {
        LOG(FATAL) << "Not enough statically allocated clean-up function slots, "
                      "consider enlarge kNumCleanupFns";
    }
    cleanup_fns[idx] = fn;
}

void SetInterruptHandler(std::function<void()> fn) {
    if (sigint_handler) {
        LOG(FATAL) << "Interrupt handler can only be set once!";
    }
    sigint_handler = fn;
}

}  // namespace base
}  // namespace faas
