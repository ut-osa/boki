#pragma once

#include "common/flags.h"

ABSL_DECLARE_FLAG(int, slog_local_cut_interval_us);
ABSL_DECLARE_FLAG(int, slog_global_cut_interval_us);
ABSL_DECLARE_FLAG(size_t, slog_log_space_hash_tokens);

ABSL_DECLARE_FLAG(bool, slog_enable_statecheck);
ABSL_DECLARE_FLAG(int, slog_statecheck_interval_sec);

ABSL_DECLARE_FLAG(bool, slog_engine_enable_cache);
ABSL_DECLARE_FLAG(int, slog_engine_cache_cap_mb);

ABSL_DECLARE_FLAG(std::string, slog_storage_backend);
ABSL_DECLARE_FLAG(int, slog_storage_bgthread_interval_ms);
ABSL_DECLARE_FLAG(size_t, slog_storage_max_live_entries);
