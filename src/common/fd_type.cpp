#include "common/fd_type.h"

#include "utils/fs.h"

#include <sys/stat.h>
#include <sys/socket.h>

namespace faas {

namespace {

FileDescriptorType InferFileType(const std::string& file_path) {
    struct stat statbuf;
    if (stat(file_path.c_str(), &statbuf) != 0) {
        PLOG_F(WARNING, "stat failed for {}", file_path);
        return FileDescriptorType::kInvalid;
    }
    if (S_ISREG(statbuf.st_mode) != 0) {
        return FileDescriptorType::kFile;
    }
    if (S_ISDIR(statbuf.st_mode) != 0) {
        return FileDescriptorType::kDirectory;
    }
    if (S_ISFIFO(statbuf.st_mode) != 0) {
        return FileDescriptorType::kFifo;
    }
    if (S_ISSOCK(statbuf.st_mode) != 0) {
        return FileDescriptorType::kUnixSocket;
    }
    LOG_F(WARNING, "Failed to determine type for {}: st_mode={:#010x}",
          file_path, statbuf.st_mode);
    return FileDescriptorType::kInvalid;
}

FileDescriptorType InferSocketType(int sockfd) {
    int sock_type;
    socklen_t length = sizeof(int);
    if (getsockopt(sockfd, SOL_SOCKET, SO_TYPE, &sock_type, &length) != 0) {
        PLOG_F(ERROR, "getsockopt failed for {}", sockfd);
        return FileDescriptorType::kInvalid;
    }
    if (sock_type == SOCK_STREAM) {
        struct sockaddr sockaddr;
        socklen_t addrlen = sizeof(struct sockaddr);
        if (getsockname(sockfd, &sockaddr, &addrlen) != 0) {
            PLOG_F(ERROR, "getsockname failed for {}", sockfd);
            return FileDescriptorType::kInvalid;
        }
        if (sockaddr.sa_family == AF_UNIX) {
            return FileDescriptorType::kUnixSocket;
        }
        if (sockaddr.sa_family == AF_INET) {
            return FileDescriptorType::kTcpSocket;
        }
        if (sockaddr.sa_family == AF_INET6) {
            return FileDescriptorType::kTcp6Socket;
        }
        return FileDescriptorType::kInvalid;
    }
    if (sock_type == SOCK_DGRAM) {
        return FileDescriptorType::kUdpSocket;
    }
    return FileDescriptorType::kInvalid;
}

}  // namespace

FileDescriptorType FileDescriptorUtils::InferType(int fd) {
    std::string file_path;
    if (!fs_utils::ReadLink(fmt::format("/proc/self/fd/{}", fd), &file_path)) {
        return FileDescriptorType::kInvalid;
    }
    if (absl::StartsWith(file_path, "/")) {
        if (absl::StartsWith(file_path, "/dev")) {
            return FileDescriptorType::kDev;
        }
        return InferFileType(file_path);
    }
    if (absl::StartsWith(file_path, "socket:")) {
        return InferSocketType(fd);
    }
    if (absl::StartsWith(file_path, "pipe:")) {
        return FileDescriptorType::kPipe;
    }
    if (absl::StartsWith(file_path, "anon_inode:")) {
        std::string_view type_str = absl::StripPrefix(file_path, "anon_inode:");
        if (type_str == "[eventfd]") {
            return FileDescriptorType::kEventFd;
        } else if (type_str == "[timerfd]") {
            return FileDescriptorType::kTimerFd;
        }
    }
    LOG_F(WARNING, "Failed to determine fd type for {}: {}", fd, file_path);
    return FileDescriptorType::kInvalid;
}

}  // namespace faas
