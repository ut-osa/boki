#pragma once

#include "base/common.h"

#include <netinet/in.h>

namespace faas {
namespace utils {

// Return sockfd on success, and return -1 on error
int UnixDomainSocketBindAndListen(std::string_view path, int backlog = 4);
int UnixDomainSocketConnect(std::string_view path);
int TcpSocketBindAndListen(std::string_view addr, uint16_t port, int backlog = 4);
int TcpSocketConnect(std::string_view addr, uint16_t port);
int Tcp6SocketBindAndListen(std::string_view ip, uint16_t port, int backlog = 4);
int Tcp6SocketConnect(std::string_view ip, uint16_t port);

bool SetTcpSocketNoDelay(int sockfd);
bool SetTcpSocketKeepAlive(int sockfd);

bool ResolveHost(std::string_view host_or_ip, struct in_addr* addr);

// Will use `getaddrinfo` to resolve IP address if necessary
bool FillTcpSocketAddr(struct sockaddr_in* addr, std::string_view host_or_ip, uint16_t port);

// `addr_str` assumed to be "[host]:[port]"
bool ParseHostPort(std::string_view addr_str, std::string_view* host, uint16_t* port);

bool NetworkOpWithRetry(int max_retry, int sleep_sec, std::function<bool()> fn);

}  // namespace utils
}  // namespace faas
