#include "base/init.h"
#include "base/common.h"
#include "common/flags.h"
#include "utils/env_variables.h"
#include "log/sequencer.h"

#include <signal.h>

ABSL_FLAG(int, node_id, -1,
          "My node ID. Also settable through environment variable FAAS_NODE_ID.");

namespace faas {

static std::atomic<server::ServerBase*> server_ptr{nullptr};
static void SignalHandlerToStopServer(int signal) {
    server::ServerBase* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

void SequencerMain(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopServer);
    base::InitMain(argc, argv);
    flags::PopulateHostnameIfEmpty();

    int node_id = absl::GetFlag(FLAGS_node_id);
    if (node_id == -1) {
        node_id = utils::GetEnvVariableAsInt("FAAS_NODE_ID", -1);
    }
    if (node_id == -1) {
        LOG(FATAL) << "Node ID not set!";
    }
    auto sequencer = std::make_unique<log::Sequencer>(node_id);

    sequencer->Start();
    server_ptr.store(sequencer.get());
    sequencer->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::SequencerMain(argc, argv);
    return 0;
}
