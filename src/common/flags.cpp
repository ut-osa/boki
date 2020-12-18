#include "common/flags.h"

ABSL_FLAG(int, socket_listen_backlog, 64, "Backlog for listen");
ABSL_FLAG(bool, tcp_enable_nodelay, true, "Enable TCP_NODELAY");
ABSL_FLAG(bool, tcp_enable_keepalive, true, "Enable TCP keep-alive");

ABSL_FLAG(std::string, slog_storage_backend, "inmem", "inmem or rocksdb");
ABSL_FLAG(std::string, slog_storage_datadir, "", "");
ABSL_FLAG(bool, slog_enable_statecheck, false, "");
ABSL_FLAG(int, slog_statecheck_interval_sec, 10, "");
