#include "base/init.h"
#include "base/common.h"
#include "sequencer/server.h"

#include <signal.h>
#include <absl/flags/flag.h>

ABSL_FLAG(std::string, listen_addr, "0.0.0.0", "Address to listen");
ABSL_FLAG(int, engine_conn_port, 10007, "Port for engine connections");

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

    auto server = std::make_unique<faas::sequencer::Server>();
    server->set_address(absl::GetFlag(FLAGS_listen_addr));
    server->set_engine_conn_port(absl::GetFlag(FLAGS_engine_conn_port));

    server->Start();
    server_ptr.store(server.get());
    server->WaitForFinish();

    return 0;
}
