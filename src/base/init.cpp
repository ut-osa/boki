#include "base/diagnostic.h"
#include "base/init.h"
#include "base/logging.h"
#include "base/thread.h"
#include "utils/docker.h"
#include "utils/fs.h"

#include <stdio.h>
#include <string.h>
#include <signal.h>

__BEGIN_THIRD_PARTY_HEADERS

#include <absl/flags/flag.h>
#include <absl/flags/parse.h>
#include <absl/flags/reflection.h>
#include <absl/debugging/symbolize.h>
#include <absl/debugging/failure_signal_handler.h>

__END_THIRD_PARTY_HEADERS

ABSL_FLAG(int, v, 0, "Show all VLOG(m) messages for m <= this.");
ABSL_FLAG(std::string, log_path, "",
          "Set file path for logging. If not set, stderr is used.");
ABSL_FLAG(bool, alsologtostderr, false,
          "If set, will log to stderr in addition to log_path");
ABSL_FLAG(std::string, profile, "", "A profile file used for setting flags");

#define RAW_CHECK(EXPR, MSG)             \
    do {                                 \
        if (!(EXPR)) {                   \
            fprintf(stderr, MSG "\n");   \
            exit(EXIT_FAILURE);          \
        }                                \
    } while (0)

extern char** environ;

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

static bool SetFlag(std::string_view flag_line) {
    std::vector<std::string_view> parts = absl::StrSplit(flag_line, ' ', absl::SkipWhitespace());
    if (parts.size() == 0) {
        return false;
    }
    std::string_view flag_name = parts[0];
    absl::CommandLineFlag* flag = absl::FindCommandLineFlag(flag_name);
    if (flag == nullptr) {
        LOG_F(ERROR, "Failed to find flag with name {}", flag_name);
        return false;
    }
    std::string_view value = (parts.size() > 1) ? parts[1] : "true";
    std::string error_msg;
    if (!flag->ParseFrom(value, &error_msg)) {
        LOG_F(ERROR, "Failed to set flag {} with {}: {} ", flag_name, value, error_msg);
        return false;
    }
    return true;
}

static void PrintAllFlags() {
    auto all_flags = absl::GetAllFlags();
    if (all_flags.empty()) {
        return;
    }
    std::stringstream stream;
    for (const auto& [flag_name, flag] : all_flags) {
        if (absl::EndsWith(flag->Filename(), "absl/flags/parse.cc")) {
            continue;
        }
        stream << flag_name << ": " << flag->CurrentValue() << '\n';
    }
    LOG(INFO) << fmt::format("All command line flags ({} in total):\n", all_flags.size())
              << stream.str() << "[END ALL FLAGS]";
}

static void PrintAllFaaSEnvs() {
    for (char** s = environ; *s; s++) {
        std::string_view env_str(*s);
        if (absl::StartsWith(env_str, "FAAS_")) {
            LOG(INFO) << "FaaS env: " << env_str;
        }
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

    Thread::RegisterMainThread();

    if (positional_args == nullptr && unparsed_args.size() > 1) {
        LOG(FATAL) << "This program does not accept positional arguments";
    }
    if (positional_args != nullptr) {
        positional_args->clear();
        for (size_t i = 1; i < unparsed_args.size(); i++) {
            positional_args->push_back(unparsed_args[i]);
        }
    }

    std::string profile_file = absl::GetFlag(FLAGS_profile);
    if (!profile_file.empty()) {
        std::string contents;
        if (!fs_utils::ReadContents(profile_file, &contents)) {
            LOG(FATAL) << "Failed to read from profile " << profile_file;
        }
        std::vector<std::string_view> flag_lines = absl::StrSplit(
            contents, '\n', absl::SkipWhitespace());
        for (std::string_view line : flag_lines) {
            if (!SetFlag(line)) {
                LOG(FATAL) << "Invalid profile line: " << line;
            }
        }
    }

    PrintAllFlags();
    PrintAllFaaSEnvs();
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
