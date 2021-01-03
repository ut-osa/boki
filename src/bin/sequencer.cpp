#include "base/init.h"
#include "base/common.h"
#include "utils/env_variables.h"
#include "sequencer/server.h"

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, raft_datadir, "", "Path to saving raft data");
ABSL_FLAG(std::string, config_file, "", "Path to config file of sequencers");
ABSL_FLAG(int, sequencer_id, -1, "My sequencer ID");

namespace faas {

static std::atomic<sequencer::Server*> server_ptr{nullptr};
static void SignalHandlerToStopServer(int signal) {
    sequencer::Server* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

void SequencerMain(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopServer);
    base::InitMain(argc, argv);

    int sequencer_id = absl::GetFlag(FLAGS_sequencer_id);
    if (sequencer_id == -1) {
        sequencer_id = utils::GetEnvVariableAsInt("FAAS_SEQUENCER_ID", -1);
    }
    auto server = std::make_unique<sequencer::Server>(
        gsl::narrow_cast<uint16_t>(sequencer_id));
    server->set_config_path(absl::GetFlag(FLAGS_config_file));
    server->set_raft_data_dir(absl::GetFlag(FLAGS_raft_datadir));

    server->Start();
    server_ptr.store(server.get());
    server->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::SequencerMain(argc, argv);
    return 0;
}
