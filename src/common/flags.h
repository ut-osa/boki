#pragma once

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

ABSL_DECLARE_FLAG(std::string, listen_addr);
ABSL_DECLARE_FLAG(std::string, hostname);
ABSL_DECLARE_FLAG(int, num_io_workers);
ABSL_DECLARE_FLAG(int, message_conn_per_worker);
ABSL_DECLARE_FLAG(int, socket_listen_backlog);
ABSL_DECLARE_FLAG(bool, tcp_enable_nodelay);
ABSL_DECLARE_FLAG(bool, tcp_enable_keepalive);

ABSL_DECLARE_FLAG(std::string, zookeeper_host);
ABSL_DECLARE_FLAG(std::string, zookeeper_root_path);

namespace faas {
namespace flags {

void PopulateHostnameIfEmpty();

}  // namespace flags
}  // namespace faas
