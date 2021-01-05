#include "common/flags.h"

#include "utils/procfs.h"

ABSL_FLAG(std::string, listen_addr, "0.0.0.0",
          "Address to listen for external TCP connections");
ABSL_FLAG(std::string, hostname, "",
          "Hostname for other components to connect. "
          "If set to empty, /proc/sys/kernel/hostname will be used.");
ABSL_FLAG(int, num_io_workers, 1, "Number of IO workers.");
ABSL_FLAG(int, message_conn_per_worker, 8,
          "Number of connections for message passing per IO worker.");
ABSL_FLAG(int, socket_listen_backlog, 64, "Backlog for listen");
ABSL_FLAG(bool, tcp_enable_nodelay, true, "Enable TCP_NODELAY");
ABSL_FLAG(bool, tcp_enable_keepalive, true, "Enable TCP keep-alive");

ABSL_FLAG(std::string, zookeeper_host, "localhost:2181", "ZooKeeper host");
ABSL_FLAG(std::string, zookeeper_root_path, "/faas", "Root path for all znodes");

ABSL_FLAG(std::string, slog_storage_backend, "inmem", "inmem or rocksdb");
ABSL_FLAG(std::string, slog_storage_datadir, "", "");
ABSL_FLAG(bool, slog_enable_statecheck, false, "");
ABSL_FLAG(int, slog_statecheck_interval_sec, 10, "");

namespace faas {
namespace flags {

void PopulateHostnameIfEmpty() {
    if (absl::GetFlag(FLAGS_hostname).empty()) {
        absl::SetFlag(&FLAGS_hostname, procfs_utils::ReadHostname());
    }
}

}  // namespace flags
}  // namespace faas
