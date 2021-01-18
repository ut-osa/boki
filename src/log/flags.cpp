#include "log/flags.h"

ABSL_FLAG(int, slog_local_cut_interval_us, 1000, "");
ABSL_FLAG(int, slog_global_cut_interval_us, 1000, "");

ABSL_FLAG(bool, slog_enable_statecheck, false, "");
ABSL_FLAG(int, slog_statecheck_interval_sec, 10, "");

ABSL_FLAG(std::string, slog_storage_backend, "rocksdb", "rocskdb, tkrzw_hashdbm");
ABSL_FLAG(int, slog_storage_bgthread_interval_ms, 1, "");
ABSL_FLAG(size_t, slog_storage_max_live_entries, 65536, "");
