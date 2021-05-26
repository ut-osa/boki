#include "common/flags.h"

ABSL_FLAG(std::string, listen_addr, "0.0.0.0",
          "Address to listen for external TCP connections");
ABSL_FLAG(std::string, listen_iface, "lo",
          "Interface to listen for message connections");
ABSL_FLAG(int, num_io_workers, 1, "Number of IO workers.");
ABSL_FLAG(int, message_conn_per_worker, 8,
          "Number of connections for message passing per IO worker.");
ABSL_FLAG(int, socket_listen_backlog, 64, "Backlog for listen");
ABSL_FLAG(bool, tcp_enable_reuseport, false, "Enable SO_REUSEPORT");
ABSL_FLAG(bool, tcp_enable_nodelay, true, "Enable TCP_NODELAY");
ABSL_FLAG(bool, tcp_enable_keepalive, true, "Enable TCP keep-alive");

ABSL_FLAG(std::string, zookeeper_host, "localhost:2181", "ZooKeeper host");
ABSL_FLAG(std::string, zookeeper_root_path, "/faas", "Root path for all znodes");

ABSL_FLAG(bool, enable_journal, false,
          "Enable IO worker journals (for write-ahead logging)");
ABSL_FLAG(std::string, journal_save_path, "",
          "The directory for storing journal files");
ABSL_FLAG(size_t, journal_file_max_size_mb, 256,
          "Maximum size (in MB) of individual journal file");
ABSL_FLAG(size_t, journal_cap_per_worker, 4,
          "Maximum number of journal files per IO worker");
