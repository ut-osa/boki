#pragma once

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

#include "common/flags.h"

ABSL_DECLARE_FLAG(int, gateway_conn_per_worker);
ABSL_DECLARE_FLAG(int, sequencer_conn_per_worker);
ABSL_DECLARE_FLAG(int, shared_log_conn_per_worker);

ABSL_DECLARE_FLAG(int, io_uring_entries);
ABSL_DECLARE_FLAG(int, io_uring_fd_slots);
ABSL_DECLARE_FLAG(bool, io_uring_sqpoll);
ABSL_DECLARE_FLAG(int, io_uring_sq_thread_idle_ms);
ABSL_DECLARE_FLAG(int, io_uring_cq_nr_wait);
ABSL_DECLARE_FLAG(int, io_uring_cq_wait_timeout_us);

ABSL_DECLARE_FLAG(bool, enable_monitor);
ABSL_DECLARE_FLAG(bool, func_worker_use_engine_socket);
ABSL_DECLARE_FLAG(bool, use_fifo_for_nested_call);
ABSL_DECLARE_FLAG(bool, func_worker_pipe_direct_write);

ABSL_DECLARE_FLAG(double, max_relative_queueing_delay);
ABSL_DECLARE_FLAG(double, concurrency_limit_coef);
ABSL_DECLARE_FLAG(double, expected_concurrency_coef);
ABSL_DECLARE_FLAG(int, min_worker_request_interval_ms);
ABSL_DECLARE_FLAG(bool, always_request_worker_if_possible);
ABSL_DECLARE_FLAG(bool, disable_concurrency_limiter);

ABSL_DECLARE_FLAG(double, instant_rps_p_norm);
ABSL_DECLARE_FLAG(double, instant_rps_ema_alpha);
ABSL_DECLARE_FLAG(double, instant_rps_ema_tau_ms);

ABSL_DECLARE_FLAG(bool, enable_shared_log);
ABSL_DECLARE_FLAG(int, shared_log_num_replicas);
ABSL_DECLARE_FLAG(int, shared_log_local_cut_interval_us);
ABSL_DECLARE_FLAG(int, shared_log_global_cut_interval_us);
