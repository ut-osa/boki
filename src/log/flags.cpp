#include "log/flags.h"

ABSL_FLAG(size_t, slog_num_replicas, 2, "");
ABSL_FLAG(int, slog_local_cut_interval_us, 1000, "");
ABSL_FLAG(int, slog_global_cut_interval_us, 1000, "");

ABSL_FLAG(size_t, slog_storage_max_live_entries, 65536, "");
