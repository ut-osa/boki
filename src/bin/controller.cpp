#include "base/init.h"
#include "base/common.h"
#include "common/flags.h"
#include "log/controller.h"

#include <signal.h>

ABSL_FLAG(size_t, metalog_replicas, 3, "Replicas for meta logs");
ABSL_FLAG(size_t, userlog_replicas, 3, "Replicas for users' logs");
ABSL_FLAG(size_t, index_replicas, 3, "Replicas for log index");

namespace faas {

static std::atomic<log::Controller*> controller_ptr{nullptr};
static void SignalHandlerToStopController(int signal) {
    log::Controller* controller = controller_ptr.exchange(nullptr);
    if (controller != nullptr) {
        controller->ScheduleStop();
    }
}

void ControllerMain(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopController);
    base::InitMain(argc, argv);

    auto controller = std::make_unique<log::Controller>();

    controller->set_metalog_replicas(absl::GetFlag(FLAGS_metalog_replicas));
    controller->set_userlog_replicas(absl::GetFlag(FLAGS_userlog_replicas));
    controller->set_index_replicas(absl::GetFlag(FLAGS_index_replicas));

    controller->Start();
    controller_ptr.store(controller.get());
    controller->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::ControllerMain(argc, argv);
    return 0;
}
