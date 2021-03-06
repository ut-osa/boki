#define __FAAS_CPP_WORKER_SRC
#include "base/common.h"
#include "base/logging.h"
#include "ipc/base.h"
#include "utils/env_variables.h"
#include "worker/v1/func_worker.h"

namespace faas {

void FuncWorkerV1Main(int argc, char* argv[]) {
    if (argc != 2) {
        fprintf(stderr, "The only argument should be path to the function library\n");
        exit(EXIT_FAILURE);
    }

    logging::Init(utils::GetEnvVariableAsInt("FAAS_VLOG_LEVEL", 0));
    ipc::SetRootPathForIpc(
        utils::GetEnvVariable("FAAS_ROOT_PATH_FOR_IPC", "/dev/shm/faas_ipc"));

    auto func_worker = std::make_unique<worker_v1::FuncWorker>();
    func_worker->set_func_id(
        utils::GetEnvVariableAsInt("FAAS_FUNC_ID", -1));
    func_worker->set_fprocess_id(
        utils::GetEnvVariableAsInt("FAAS_FPROCESS_ID", -1));
    func_worker->set_client_id(
        utils::GetEnvVariableAsInt("FAAS_CLIENT_ID", 0));
    func_worker->set_message_pipe_fd(
        utils::GetEnvVariableAsInt("FAAS_MSG_PIPE_FD", -1));
    if (utils::GetEnvVariableAsInt("FAAS_USE_ENGINE_SOCKET", 0) == 1) {
        func_worker->enable_use_engine_socket();
    }
    func_worker->set_engine_tcp_port(
        utils::GetEnvVariableAsInt("FAAS_ENGINE_TCP_PORT", -1));
    func_worker->set_func_library_path(argv[1]);
    func_worker->Serve();
}

}  // namespace faas

int main(int argc, char* argv[]) {
    faas::FuncWorkerV1Main(argc, argv);
    return 0;
}
