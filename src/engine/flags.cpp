#include "engine/flags.h"

ABSL_FLAG(size_t, gateway_conn_per_worker, 2, "");
ABSL_FLAG(size_t, sequencer_conn_per_worker, 2, "");
ABSL_FLAG(size_t, shared_log_conn_per_worker, 2, "");

ABSL_FLAG(size_t, io_uring_entries, 2048, "");
ABSL_FLAG(size_t, io_uring_fd_slots, 1024, "");
ABSL_FLAG(bool, io_uring_sqpoll, false, "");
ABSL_FLAG(int, io_uring_sq_thread_idle_ms, 1, "");
ABSL_FLAG(int, io_uring_cq_nr_wait, 1, "");
ABSL_FLAG(int, io_uring_cq_wait_timeout_us, 0, "");

ABSL_FLAG(bool, enable_monitor, false, "");
ABSL_FLAG(bool, func_worker_use_engine_socket, false, "");
ABSL_FLAG(bool, use_fifo_for_nested_call, false, "");
ABSL_FLAG(bool, func_worker_pipe_direct_write, false, "");

ABSL_FLAG(double, max_relative_queueing_delay, 0.0, "");
ABSL_FLAG(double, concurrency_limit_coef, 1.0, "");
ABSL_FLAG(double, expected_concurrency_coef, 1.0, "");
ABSL_FLAG(int, min_worker_request_interval_ms, 200, "");
ABSL_FLAG(bool, always_request_worker_if_possible, false, "");
ABSL_FLAG(bool, disable_concurrency_limiter, false, "");

ABSL_FLAG(double, instant_rps_p_norm, 1.0, "");
ABSL_FLAG(double, instant_rps_ema_alpha, 0.001, "");
ABSL_FLAG(double, instant_rps_ema_tau_ms, 0, "");

ABSL_FLAG(std::string, slog_storage_backend, "inmem", "inmem or rocksdb");
ABSL_FLAG(std::string, slog_storage_datadir, "", "");
ABSL_FLAG(bool, slog_enable_statecheck, false, "");
ABSL_FLAG(int, slog_statecheck_interval_sec, 10, "");
