#pragma once

#include "common/flags.h"

ABSL_DECLARE_FLAG(size_t, sequencer_conn_per_worker);
ABSL_DECLARE_FLAG(size_t, shared_log_conn_per_worker);

ABSL_DECLARE_FLAG(bool, enable_monitor);
ABSL_DECLARE_FLAG(bool, func_worker_use_engine_socket);
ABSL_DECLARE_FLAG(bool, use_fifo_for_nested_call);

ABSL_DECLARE_FLAG(double, max_relative_queueing_delay);
ABSL_DECLARE_FLAG(double, concurrency_limit_coef);
ABSL_DECLARE_FLAG(double, expected_concurrency_coef);
ABSL_DECLARE_FLAG(int, min_worker_request_interval_ms);
ABSL_DECLARE_FLAG(bool, always_request_worker_if_possible);
ABSL_DECLARE_FLAG(bool, disable_concurrency_limiter);

ABSL_DECLARE_FLAG(double, instant_rps_p_norm);
ABSL_DECLARE_FLAG(double, instant_rps_ema_alpha);
ABSL_DECLARE_FLAG(double, instant_rps_ema_tau_ms);
