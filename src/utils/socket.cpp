#define __FAAS_USED_IN_BINDING
#include "utils/socket.h"

#ifdef __FAAS_HAVE_ABSL
__BEGIN_THIRD_PARTY_HEADERS
#include <absl/strings/numbers.h>
__END_THIRD_PARTY_HEADERS
#endif

#ifdef __FAAS_SRC
#include "common/flags.h"
#endif

#include "utils/random.h"

#include <arpa/inet.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <netdb.h>
#include <net/if.h>

namespace faas {
namespace utils {

namespace {
static bool SetSocketOption(int sockfd, int option, int value) {
    if (setsockopt(sockfd, SOL_SOCKET, option, (const void*) &value, sizeof(int)) != 0) {
        PLOG(ERROR) << "setsockopt failed";
        return false;
    }
    return true;
}

static bool SetTcpSocketOption(int sockfd, int option, int value) {
    if (setsockopt(sockfd, IPPROTO_TCP, option, (const void*) &value, sizeof(int)) != 0) {
        PLOG(ERROR) << "setsockopt failed";
        return false;
    }
    return true;
}

static bool FillUnixSocketAddr(struct sockaddr_un* addr, std::string_view path) {
    if (path.length() >= sizeof(addr->sun_path)) {
        return false;
    }
    addr->sun_family = AF_UNIX;
    memcpy(addr->sun_path, path.data(), path.length());
    addr->sun_path[path.length()] = '\0';
    return true;
}

static bool FillTcpSocketAddr(struct sockaddr_in* addr,
                              std::string_view ip, uint16_t port) {
    addr->sin_family = AF_INET; 
    addr->sin_port = htons(port);
    if (inet_aton(std::string(ip).c_str(), &addr->sin_addr) != 1) {
        return false;
    }
    return true;
}

static bool FillTcp6SocketAddr(struct sockaddr_in6* addr,
                               std::string_view ip, uint16_t port) {
    addr->sin6_family = AF_INET6; 
    addr->sin6_port = htons(port);
    addr->sin6_flowinfo = 0;
    if (inet_pton(AF_INET6, std::string(ip).c_str(), &addr->sin6_addr) != 1) {
        return false;
    }
    addr->sin6_scope_id = 0;
    return true;
}

static bool ResolveHostInternal(std::string_view host_or_ip, struct in_addr* addr) {
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
}

bool SocketListen(int sockfd, int backlog) {
    if (listen(sockfd, backlog) != 0) {
        PLOG(ERROR) << "Failed to listen with backlog " << backlog;
        return false;
    }
    return true;
}

int UnixSocketBindAndListen(std::string_view path, int backlog) {
    struct sockaddr_un addr;
    if (!FillUnixSocketAddr(&addr, path)) {
        LOG(ERROR) << "Failed to fill Unix socket path: " << path;
        return -1;
    }
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create Unix socket";
        return -1;
    }
    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        PLOG(ERROR) << "Failed to bind to " << path;
        close(fd);
        return -1;
    }
    if (!SocketListen(fd, backlog)) {
        close(fd);
        return -1;
    }
    return fd;
}

int UnixSocketConnect(std::string_view path) {
    struct sockaddr_un addr;
    if (!FillUnixSocketAddr(&addr, path)) {
        LOG(ERROR) << "Failed to fill Unix socket path: " << path;
        return -1;
    }
    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
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

int TcpSocketBindAndListen(std::string_view ip, uint16_t port, int backlog) {
    struct sockaddr_in sockaddr;
    if (!FillTcpSocketAddr(&sockaddr, ip, port)) {
        LOG_F(ERROR, "Failed to fill socket addr: {}:{}", ip, port);
        return -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET socket";
        return -1;
    }
#ifdef __FAAS_SRC
    if (absl::GetFlag(FLAGS_tcp_enable_reuseport)) {
        CHECK(SetSocketOption(fd, SO_REUSEPORT, 1));
    }
#endif
    if (bind(fd, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0) {
        PLOG_F(ERROR, "Failed to bind to {}:{}", ip, port);
        close(fd);
        return -1;
    }
    if (!SocketListen(fd, backlog)) {
        close(fd);
        return -1;
    }
    return fd;
}

int TcpSocketConnect(std::string_view ip, uint16_t port) {
    struct sockaddr_in sockaddr;
    if (!FillTcpSocketAddr(&sockaddr, ip, port)) {
        LOG_F(ERROR, "Failed to fill socket addr: {}:{}", ip, port);
        return -1;
    }
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET socket";
        return -1;
    }
    if (connect(fd, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0) {
        PLOG_F(ERROR, "Failed to connect to {}:{}", ip, port);
        close(fd);
        return -1;
    }
    return fd;
}

int Tcp6SocketBindAndListen(std::string_view ip, uint16_t port, int backlog) {
    struct sockaddr_in6 sockaddr;
    if (!FillTcp6SocketAddr(&sockaddr, ip, port)) {
        LOG_F(ERROR, "Failed to fill socket addr: {}:{}", ip, port);
        return -1;
    }
    int fd = socket(AF_INET6, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET6 socket";
        return -1;
    }
    if (bind(fd, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0) {
        PLOG_F(ERROR, "Failed to bind to {}:{}", ip, port);
        close(fd);
        return -1;
    }
    if (!SocketListen(fd, backlog)) {
        close(fd);
        return -1;
    }
    return fd;
}

int Tcp6SocketConnect(std::string_view ip, uint16_t port) {
    struct sockaddr_in6 sockaddr;
    if (!FillTcp6SocketAddr(&sockaddr, ip, port)) {
        LOG_F(ERROR, "Failed to fill socket addr: {}:{}", ip, port);
        return -1;
    }
    int fd = socket(AF_INET6, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET6 socket";
        return -1;
    }
#ifdef __FAAS_SRC
    if (absl::GetFlag(FLAGS_tcp_enable_reuseport)) {
        CHECK(SetSocketOption(fd, SO_REUSEPORT, 1));
    }
#endif
    if (connect(fd, (struct sockaddr*)&sockaddr, sizeof(sockaddr)) != 0) {
        PLOG_F(ERROR, "Failed to connect to {}:{}", ip, port);
        close(fd);
        return -1;
    }
    return fd;
}

int TcpSocketBindArbitraryPort(std::string_view ip, uint16_t* port) {
    static constexpr int kMaxRetires = 10;
    static constexpr int kMinPortNum = 20000;
    static constexpr int kMaxPortNum = 30000;
    int sockfd;
    bool success = NetworkOpWithRetry(
        kMaxRetires, /* sleep_sec= */ 0,
        [&sockfd, ip, port] () -> bool {
            struct sockaddr_in sockaddr;
            uint16_t random_port = gsl::narrow_cast<uint16_t>(
                utils::GetRandomInt(kMinPortNum, kMaxPortNum));
            if (!FillTcpSocketAddr(&sockaddr, ip, random_port)) {
                LOG_F(ERROR, "Failed to fill socket addr: {}:{}", ip, random_port);
                return false;
            }
            sockfd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            if (sockfd == -1) {
                PLOG(ERROR) << "Failed to create AF_INET socket";
                return false;
            }
            int ret = bind(sockfd, (struct sockaddr*)&sockaddr, sizeof(sockaddr));
            if (ret == 0) {
                *port = random_port;
                return true;
            }
            close(sockfd);
            if (errno != EADDRINUSE) {
                PLOG_F(ERROR, "Failed to bind to {}:{}", ip, random_port);
            }
            return false;
        }
    );
    return success ? sockfd : -1;
}

bool SetTcpSocketNoDelay(int sockfd) {
    return SetTcpSocketOption(sockfd, TCP_NODELAY, 1);
}

bool SetTcpSocketKeepAlive(int sockfd) {
    return SetSocketOption(sockfd, SO_KEEPALIVE, 1);
}

bool ResolveHost(std::string_view host_or_ip, std::string* ip) {
    struct in_addr addr;
    if (!ResolveHostInternal(host_or_ip, &addr)) {
        return false;
    }
    *ip = inet_ntoa(addr);
    return true;
}

bool ResolveTcpAddr(struct sockaddr_in* addr, std::string_view addr_str) {

    size_t pos = addr_str.find_last_of(":");
    if (pos == std::string::npos) {
        return false;
    }
    std::string_view host = addr_str.substr(0, pos);
    int parsed_port;
#ifdef __FAAS_HAVE_ABSL
    if (!absl::SimpleAtoi(addr_str.substr(pos + 1), &parsed_port)) {
        return false;
    }
#else
    NOT_IMPLEMENTED();
#endif
    addr->sin_family = AF_INET; 
    addr->sin_port = htons(gsl::narrow_cast<uint16_t>(parsed_port));
    return ResolveHostInternal(host, &addr->sin_addr);
}

bool ResolveInterfaceIp(std::string_view interface, std::string* ip) {
    int fd = socket(AF_INET, SOCK_DGRAM, 0);
    if (fd == -1) {
        PLOG(ERROR) << "Failed to create AF_INET socket";
        return false;
    }
    auto cleanup = gsl::finally([fd] { close(fd); });
    struct ifreq ifr;
    ifr.ifr_addr.sa_family = AF_INET;
    size_t length = interface.length();
    CHECK_LT(length, size_t{IFNAMSIZ});
    memcpy(ifr.ifr_name, interface.data(), length);
    ifr.ifr_name[length] = '\0';
    if (ioctl(fd, SIOCGIFADDR, &ifr) != 0) {
        PLOG(ERROR) << "ioctl failed";
        return false;
    }
    struct sockaddr_in* addr = (struct sockaddr_in*) &ifr.ifr_addr;
    *ip = inet_ntoa(addr->sin_addr);
    return true;
}

bool NetworkOpWithRetry(int max_retry, int sleep_sec, std::function<bool()> fn) {
    int remaining_retries = max_retry;
    while (--remaining_retries > 0) {
        if (fn()) {
            return true;
        }
        if (sleep_sec > 0) {
            sleep(static_cast<unsigned>(sleep_sec));
        }
    }
    return false;
}

}  // namespace utils
}  // namespace faas
