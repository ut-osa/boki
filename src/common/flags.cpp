#include "common/flags.h"

ABSL_FLAG(bool, tcp_enable_nodelay, true, "Enable TCP_NODELAY");
ABSL_FLAG(bool, tcp_enable_keepalive, true, "Enable TCP keep-alive");

ABSL_FLAG(int, io_uring_entries, 128, "");
ABSL_FLAG(int, io_uring_fd_slots, 128, "");
ABSL_FLAG(bool, io_uring_sqpoll, false, "");
ABSL_FLAG(int, io_uring_sq_thread_idle_ms, 1, "");
ABSL_FLAG(int, io_uring_cq_nr_wait, 1, "");
ABSL_FLAG(int, io_uring_cq_wait_timeout_us, 0, "");

ABSL_FLAG(bool, enable_monitor, false, "");
ABSL_FLAG(bool, func_worker_use_engine_socket, false, "");
ABSL_FLAG(bool, use_fifo_for_nested_call, false, "");

ABSL_FLAG(bool, enable_shared_log, false, "");
ABSL_FLAG(int, shared_log_num_replicas, 2, "");
ABSL_FLAG(int, shared_log_local_cut_interval_us, 1000, "");
ABSL_FLAG(int, shared_log_global_cut_interval_us, 1000, "");
