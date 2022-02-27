#include "log/flags.h"

ABSL_FLAG(int, slog_local_cut_interval_us, 1000, "");
ABSL_FLAG(int, slog_global_cut_interval_us, 1000, "");
ABSL_FLAG(size_t, slog_log_space_hash_tokens, 128, "");
ABSL_FLAG(size_t, slog_num_tail_metalog_entries, 32, "");

ABSL_FLAG(bool, slog_enable_statecheck, false, "");
ABSL_FLAG(int, slog_statecheck_interval_sec, 10, "");

ABSL_FLAG(bool, slog_engine_force_remote_index, false, "");
ABSL_FLAG(float, slog_engine_prob_remote_index, 0.0f, "");
ABSL_FLAG(bool, slog_engine_enable_cache, false, "");
ABSL_FLAG(int, slog_engine_cache_cap_mb, 1024, "");
ABSL_FLAG(bool, slog_engine_propagate_auxdata, false, "");

ABSL_FLAG(bool, slog_storage_enable_journal, false, "");
ABSL_FLAG(int, slog_storage_cache_cap_mb, 1024, "");
ABSL_FLAG(std::string, slog_storage_backend, "rocksdb",
          "rocskdb, tkrzw_hash, tkrzw_tree, tkrzw_skip, or journal");
ABSL_FLAG(size_t, slog_storage_flusher_threads, 1, "");
ABSL_FLAG(size_t, slog_storage_max_live_entries, 10000, "");
ABSL_FLAG(size_t, slog_storage_target_live_entries, 1000, "");
ABSL_FLAG(int, slog_storage_trim_gc_internval_ms, 100, "");
ABSL_FLAG(int, slog_storage_indexer_flush_internval_ms, 0, "");
