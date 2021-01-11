#pragma once

#include "common/flags.h"

ABSL_DECLARE_FLAG(size_t, slog_num_replicas);
ABSL_DECLARE_FLAG(int, slog_local_cut_interval_us);
ABSL_DECLARE_FLAG(int, slog_global_cut_interval_us);

ABSL_DECLARE_FLAG(size_t, slog_storage_max_live_entries);
