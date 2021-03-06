#pragma once

#include "base/common.h"

#include <netinet/in.h>

namespace faas {
namespace utils {

// Listen on `sockfd` with `backlog`
bool SocketListen(int sockfd, int backlog);

// Return sockfd on success, and return -1 on error
int UnixSocketBindAndListen(std::string_view path, int backlog = 4);
int UnixSocketConnect(std::string_view path);
int TcpSocketBindAndListen(std::string_view ip, uint16_t port, int backlog = 4);
int TcpSocketConnect(std::string_view ip, uint16_t port);
int Tcp6SocketBindAndListen(std::string_view ip, uint16_t port, int backlog = 4);
int Tcp6SocketConnect(std::string_view ip, uint16_t port);

// Bind an arbitrary port that is available
int TcpSocketBindArbitraryPort(std::string_view ip, uint16_t* port);

bool SetTcpSocketNoDelay(int sockfd);
bool SetTcpSocketKeepAlive(int sockfd);

// `host_or_ip` can be hostname or IP address
bool ResolveHost(std::string_view host_or_ip, std::string* ip);
// `addr_str` assumed to be "[host]:[port]"
bool ResolveTcpAddr(struct sockaddr_in* addr, std::string_view addr_str);

// Resolve the IP address of a network interface
bool ResolveInterfaceIp(std::string_view interface, std::string* ip);

bool NetworkOpWithRetry(int max_retry, int sleep_sec, std::function<bool()> fn);

}  // namespace utils
}  // namespace faas
