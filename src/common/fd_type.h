#pragma once

#ifndef __FAAS_SRC
#error common/fd_type.h cannot be included outside
#endif

#include "base/common.h"

namespace faas {

enum class FileDescriptorType {
    kInvalid = 0,
    // Normal file
    kFile,
    kDirectory,
    // Socket types
    kTcpSocket,
    kTcp6Socket,
    kUdpSocket,
    kUnixSocket,
    // Pipe types
    kPipe,
    kFifo,
    // Special files in /dev
    kDev,
    // Other types
    kEventFd,
    kTimerFd,
};

class FileDescriptorUtils {
public:
    static FileDescriptorType InferType(int fd);

    static bool IsSocketType(FileDescriptorType fd_type) {
        return fd_type == FileDescriptorType::kTcpSocket
            || fd_type == FileDescriptorType::kTcp6Socket
            || fd_type == FileDescriptorType::kUdpSocket
            || fd_type == FileDescriptorType::kUnixSocket;
    }

    static std::string_view TypeString(FileDescriptorType fd_type) {
        switch (fd_type) {
        case FileDescriptorType::kFile:
            return "file";
        case FileDescriptorType::kDirectory:
            return "dir";
        case FileDescriptorType::kTcpSocket:
            return "tcp";
        case FileDescriptorType::kTcp6Socket:
            return "tcp6";
        case FileDescriptorType::kUdpSocket:
            return "udp";
        case FileDescriptorType::kUnixSocket:
            return "unix";
        case FileDescriptorType::kPipe:
            return "pipe";
        case FileDescriptorType::kFifo:
            return "fifo";
        case FileDescriptorType::kDev:
            return "dev";
        case FileDescriptorType::kEventFd:
            return "eventfd";
        case FileDescriptorType::kTimerFd:
            return "timerfd";
        default:
            LOG(FATAL) << "Unknown fd type: " << static_cast<int>(fd_type);
        }
    }

private:
    DISALLOW_IMPLICIT_CONSTRUCTORS(FileDescriptorUtils);
};

}  // namespace faas
