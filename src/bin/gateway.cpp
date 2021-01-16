#include "base/init.h"
#include "base/common.h"
#include "gateway/server.h"

#include <absl/flags/flag.h>

ABSL_FLAG(int, http_port, 8080, "Port for HTTP connections");
ABSL_FLAG(int, grpc_port, 50051, "Port for gRPC connections");
ABSL_FLAG(std::string, func_config_file, "", "Path to function config file");

namespace faas {

static std::atomic<server::ServerBase*> server_ptr{nullptr};
static void SignalHandlerToStopServer(int signal) {
    server::ServerBase* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

void GatewayMain(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopServer);
    base::InitMain(argc, argv);

    auto server = std::make_unique<gateway::Server>();
    server->set_http_port(absl::GetFlag(FLAGS_http_port));
    server->set_grpc_port(absl::GetFlag(FLAGS_grpc_port));
    server->set_func_config_file(absl::GetFlag(FLAGS_func_config_file));

    server->Start();
    server_ptr.store(server.get());
    server->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::GatewayMain(argc, argv);
    return 0;
}
