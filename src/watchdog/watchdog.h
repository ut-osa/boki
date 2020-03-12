#pragma once

#include "base/common.h"
#include "common/protocol.h"
#include "common/func_config.h"
#include "utils/uv_utils.h"
#include "utils/shared_memory.h"
#include "utils/buffer_pool.h"
#include "watchdog/run_mode.h"
#include "watchdog/gateway_connection.h"
#include "watchdog/func_runner.h"
#include "watchdog/func_worker.h"

namespace faas {
namespace watchdog {

class Watchdog : public uv::Base {
public:
    static constexpr size_t kSubprocessPipeBufferSizeForSerializingMode = 65536;
    static constexpr size_t kSubprocessPipeBufferSizeForFuncWorkerMode = 256;

    Watchdog();
    ~Watchdog();

    void set_gateway_ipc_path(absl::string_view path) {
        gateway_ipc_path_ = std::string(path);
    }
    void set_func_id(int func_id) {
        func_id_ = func_id;
    }
    void set_fprocess(absl::string_view fprocess) {
        fprocess_ = std::string(fprocess);
    }
    void set_shared_mem_path(absl::string_view path) {
        shared_mem_path_ = std::string(path);
    }
    void set_func_config_file(absl::string_view path) {
        func_config_file_ = std::string(path);
    }
    void set_run_mode(int run_mode) {
        run_mode_ = static_cast<RunMode>(run_mode);
    }
    void set_num_func_workers(int num_func_workers) {
        num_func_workers_ = num_func_workers;
    }

    absl::string_view gateway_ipc_path() const { return gateway_ipc_path_; }
    int func_id() const { return func_id_; }
    absl::string_view shared_mem_path() const { return shared_mem_path_; }
    absl::string_view func_config_file() const { return func_config_file_; }

    absl::string_view fprocess() const { return fprocess_; }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

    void OnGatewayConnectionClose();
    void OnFuncRunnerComplete(FuncRunner* func_runner, FuncRunner::Status status);
    void OnFuncWorkerClose(FuncWorker* func_worker);

    bool OnRecvHandshakeResponse(const protocol::HandshakeResponse& response);
    void OnRecvMessage(const protocol::Message& message);

private:
    enum State { kCreated, kRunning, kStopping, kStopped };
    std::atomic<State> state_;

    std::string gateway_ipc_path_;
    int func_id_;
    std::string fprocess_;
    std::string shared_mem_path_;
    std::string func_config_file_;
    RunMode run_mode_;
    int num_func_workers_;
    uint16_t client_id_;

    uv_loop_t uv_loop_;
    uv_async_t stop_event_;
    base::Thread event_loop_thread_;

    FuncConfig func_config_;
    std::unique_ptr<utils::SharedMemory> shared_memory_;

    GatewayConnection gateway_connection_;
    std::unique_ptr<utils::BufferPool> buffer_pool_for_subprocess_pipes_;
    absl::flat_hash_map<uint64_t, std::unique_ptr<FuncRunner>> func_runners_;

    std::vector<std::unique_ptr<FuncWorker>> func_workers_;
    int next_func_worker_id_;

    void EventLoopThreadMain();
    FuncWorker* PickFuncWorker();

    DECLARE_UV_ASYNC_CB_FOR_CLASS(Stop);

    DISALLOW_COPY_AND_ASSIGN(Watchdog);
};

}  // namespace watchdog
}  // namespace faas
