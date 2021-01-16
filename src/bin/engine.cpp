#include "base/init.h"
#include "base/common.h"
#include "ipc/base.h"
#include "utils/docker.h"
#include "utils/fs.h"
#include "utils/procfs.h"
#include "utils/env_variables.h"
#include "engine/engine.h"

#include <absl/flags/flag.h>

ABSL_FLAG(int, engine_tcp_port, -1,
          "If set, Launcher and FuncWorker will communicate with engine via localhost TCP socket");
ABSL_FLAG(int, node_id, -1,
          "My node ID. If -1 is set, node ID will be automatically generated based on "
          "/proc/sys/kernel/hostname");
ABSL_FLAG(std::string, root_path_for_ipc, "/dev/shm/faas_ipc",
          "Root directory for IPCs used by FaaS");
ABSL_FLAG(std::string, func_config_file, "", "Path to function config file");
ABSL_FLAG(bool, enable_shared_log, false, "If to enable shared log.");

namespace faas {

static std::atomic<server::ServerBase*> server_ptr{nullptr};
static void SignalHandlerToStopServer(int signal) {
    server::ServerBase* server = server_ptr.exchange(nullptr);
    if (server != nullptr) {
        server->ScheduleStop();
    }
}

static uint16_t GenerateNodeId() {
    std::string hostname = procfs_utils::ReadHostname();
    uint16_t result = 0;
    for (const char ch : hostname) {
        // Let overflow happens freely here
        result = result * 177 + ch;
    }
    return result;
}

void EngineMain(int argc, char* argv[]) {
    signal(SIGINT, SignalHandlerToStopServer);
    base::InitMain(argc, argv);
    ipc::SetRootPathForIpc(absl::GetFlag(FLAGS_root_path_for_ipc), /* create= */ true);

    std::string cgroup_fs_root(utils::GetEnvVariable("FAAS_CGROUP_FS_ROOT", ""));
    if (cgroup_fs_root.length() > 0) {
        docker_utils::SetCgroupFsRoot(cgroup_fs_root);
    }

    int node_id = absl::GetFlag(FLAGS_node_id);
    if (node_id == -1) {
        node_id = utils::GetEnvVariableAsInt("FAAS_NODE_ID", -1);
    }
    if (node_id == -1) {
        node_id = GenerateNodeId();
    }
    auto engine = std::make_unique<engine::Engine>(node_id);
    engine->set_engine_tcp_port(absl::GetFlag(FLAGS_engine_tcp_port));
    engine->set_func_config_file(absl::GetFlag(FLAGS_func_config_file));

    if (absl::GetFlag(FLAGS_enable_shared_log)) {
        engine->enable_shared_log();
    }

    engine->Start();
    server_ptr.store(engine.get());
    engine->WaitForFinish();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::EngineMain(argc, argv);
    return 0;
}
