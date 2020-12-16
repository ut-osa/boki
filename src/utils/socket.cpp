#define __FAAS_USED_IN_BINDING
#include "utils/socket.h"

#ifdef __FAAS_HAVE_ABSL
#include <absl/strings/numbers.h>
#endif

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/tcp.h>
#include <netdb.h>

namespace faas {
namespace utils {

namespace {
static bool FillUnixSocketAddr(struct sockaddr_un* addr, std::string_view path) {
    if (path.length() >= sizeof(addr->sun_path)) {
        return false;
    }
    addr->sun_family = AF_UNIX;
    memcpy(addr->sun_path, path.data(), path.length());
    addr->sun_path[path.length()] = '\0';
    return true;
}

static bool FillTcp6SocketAddr(struct sockaddr_in6* addr, std::string_view ip, uint16_t port) {
    addr->sin6_family = AF_INET6; 
    addr->sin6_port = htons(port);
    addr->sin6_flowinfo = 0;
    if (inet_pton(AF_INET6, std::string(ip).c_str(), &addr->sin6_addr) != 1) {
        return false;
    }
    addr->sin6_scope_id = 0;
    return true;
}
}

int UnixDomainSocketBindAndListen(std::string_view path, int backlog) {
    struct sockaddr_un addr;
    if (!FillUnixSocketAddr(&addr, path)) {
        LOG(ERROR) << "Failed to fill Unix socket path: " << path;
        return -1;
    }
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create Unix socket";
        return -1;
    }
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        PLOG(ERROR) << "Failed to bind to " << path;
        close(fd);
        return -1;
    }
    if (listen(fd, backlog) != 0) {
        PLOG(ERROR) << "Failed to listen with backlog " << backlog;
        close(fd);
        return -1;
    }
    return fd;
}

int UnixDomainSocketConnect(std::string_view path) {
    struct sockaddr_un addr;
    if (!FillUnixSocketAddr(&addr, path)) {
        LOG(ERROR) << "Failed to fill Unix socket path: " << path;
        return -1;
    }
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create Unix socket";
        return -1;
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        PLOG(ERROR) << "Failed to connect to " << path;
        close(fd);
        return -1;
    }
    return fd;
}

int TcpSocketBindAndListen(std::string_view addr, uint16_t port, int backlog) {
    struct sockaddr_in sockaddr;
    if (!FillTcpSocketAddr(&sockaddr, addr, port)) {
        LOG(ERROR) << "Failed to fill socket addr: " << addr << ":" << port;
        return -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET socket";
        return -1;
    }
    if (bind(fd, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0) {
        PLOG(ERROR) << "Failed to bind to " << addr << ":" << port;
        close(fd);
        return -1;
    }
    if (listen(fd, backlog) != 0) {
        PLOG(ERROR) << "Failed to listen with backlog " << backlog;
        close(fd);
        return -1;
    }
    return fd;
}

int TcpSocketConnect(std::string_view addr, uint16_t port) {
    struct sockaddr_in sockaddr;
    if (!FillTcpSocketAddr(&sockaddr, addr, port)) {
        LOG(ERROR) << "Failed to fill socket addr: " << addr << ":" << port;
        return -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET socket";
        return -1;
    }
    if (connect(fd, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0) {
        PLOG(ERROR) << "Failed to connect to " << addr << ":" << port;
        close(fd);
        return -1;
    }
    return fd;
}

int Tcp6SocketBindAndListen(std::string_view ip, uint16_t port, int backlog) {
    struct sockaddr_in6 sockaddr;
    if (!FillTcp6SocketAddr(&sockaddr, ip, port)) {
        LOG(ERROR) << "Failed to fill socket addr: " << ip << ":" << port;
        return -1;
    }
    int fd = socket(AF_INET6, SOCK_STREAM, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET6 socket";
        return -1;
    }
    if (bind(fd, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0) {
        PLOG(ERROR) << "Failed to bind to " << ip << ":" << port;
        close(fd);
        return -1;
    }
    if (listen(fd, backlog) != 0) {
        PLOG(ERROR) << "Failed to listen with backlog " << backlog;
        close(fd);
        return -1;
    }
    return fd;
}

int Tcp6SocketConnect(std::string_view ip, uint16_t port) {
    struct sockaddr_in6 sockaddr;
    if (!FillTcp6SocketAddr(&sockaddr, ip, port)) {
        LOG(ERROR) << "Failed to fill socket addr: " << ip << ":" << port;
        return -1;
    }
    int fd = socket(AF_INET6, SOCK_STREAM, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET6 socket";
        return -1;
    }
    if (connect(fd, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0) {
        PLOG(ERROR) << "Failed to connect to " << ip << ":" << port;
        close(fd);
        return -1;
    }
    return fd;
}

bool SetTcpSocketNoDelay(int sockfd) {
    int flag = 1;
    if (setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY,
                   reinterpret_cast<const void*>(&flag), sizeof(int)) != 0) {
        PLOG(ERROR) << "Failed to set TCP_NODELAY";
        return false;
    }
    return true;
}

bool SetTcpSocketKeepAlive(int sockfd) {
    int flag = 1;
    if (setsockopt(sockfd, IPPROTO_TCP, SO_KEEPALIVE,
                   reinterpret_cast<const void*>(&flag), sizeof(int)) != 0) {
        PLOG(ERROR) << "Failed to set TCP_KEEPALIVE";
        return false;
    }
    return true;
}

bool ResolveHost(std::string_view host_or_ip, struct in_addr* addr) {
    // Assume host_or_ip is IP address first
    if (inet_aton(std::string(host_or_ip).c_str(), addr) == 1) {
        return true;
    }
    // Use getaddrinfo to resolve host
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags |= AI_CANONNAME;
    struct addrinfo* result;
    int ret = getaddrinfo(std::string(host_or_ip).c_str(), nullptr, &hints, &result);
    if (ret != 0) {
        if (ret != EAI_SYSTEM) {
            LOG(ERROR) << "getaddrinfo with " << host_or_ip << " failed : " << gai_strerror(ret);
        } else {
            PLOG(ERROR) << "getaddrinfo with " << host_or_ip << " failed";
        }
        return false;
    }
    auto free_freeaddrinfo_result = gsl::finally([result] () {
        freeaddrinfo(result);
    });
    while (result) {
        if (result->ai_family == AF_INET) {
            struct sockaddr_in* resolved_addr = (struct sockaddr_in*)result->ai_addr;
            *addr = resolved_addr->sin_addr;
            return true;
        }
        result = result->ai_next;
    }
    return false;
}

bool FillTcpSocketAddr(struct sockaddr_in* addr, std::string_view host_or_ip, uint16_t port) {
    addr->sin_family = AF_INET; 
    addr->sin_port = htons(port);
    return ResolveHost(host_or_ip, &addr->sin_addr);
}

bool ParseHostPort(std::string_view addr_str, std::string_view* host, uint16_t* port) {
#ifdef __FAAS_HAVE_ABSL
    size_t pos = addr_str.find_last_of(":");
    if (pos == std::string::npos) {
        return false;
    }
    *host = addr_str.substr(0, pos);
    int parsed_port;
    if (!absl::SimpleAtoi(addr_str.substr(pos + 1), &parsed_port)) {
        return false;
    }
    *port = gsl::narrow_cast<uint16_t>(parsed_port);
    return true;
#else
    NOT_IMPLEMENTED();
#endif
}

bool NetworkOpWithRetry(int max_retry, int sleep_sec, std::function<bool()> fn) {
    int remaining_retries = max_retry;
    while (--remaining_retries > 0) {
        if (fn()) {
            return true;
        }
        sleep(sleep_sec);
    }
    return false;
}

}  // namespace utils
}  // namespace faas
