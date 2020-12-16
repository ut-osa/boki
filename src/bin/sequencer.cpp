#include "base/init.h"
#include "base/common.h"
#include "utils/env_variables.h"
#include "sequencer/server.h"

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, listen_addr, "0.0.0.0", "Address to listen");
ABSL_FLAG(std::string, raft_datadir, "", "Path to saving raft data");
ABSL_FLAG(std::string, config_file, "", "Path to config file of sequencers");
ABSL_FLAG(int, sequencer_id, -1, "My sequencer ID");

static std::atomic<faas::sequencer::Server*> server_ptr(nullptr);
void SignalHandlerToStopServer(int signal) {
    faas::sequencer::Server* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

int main(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopServer);
    faas::base::InitMain(argc, argv);

    int sequencer_id = absl::GetFlag(FLAGS_sequencer_id);
    if (sequencer_id == -1) {
        sequencer_id = faas::utils::GetEnvVariableAsInt("FAAS_SEQUENCER_ID", -1);
    }
    auto server = std::make_unique<faas::sequencer::Server>(
        gsl::narrow_cast<uint16_t>(sequencer_id));
    server->set_address(absl::GetFlag(FLAGS_listen_addr));
    server->set_config_path(absl::GetFlag(FLAGS_config_file));
    server->set_raft_data_dir(absl::GetFlag(FLAGS_raft_datadir));

    server->Start();
    server_ptr.store(server.get());
    server->WaitForFinish();

    return 0;
}
