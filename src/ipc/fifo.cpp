#define __FAAS_USED_IN_BINDING
#include "ipc/fifo.h"

#include "ipc/base.h"
#include "utils/fs.h"
#include "utils/io.h"
#include "common/time.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

namespace faas {
namespace ipc {

bool FifoCreate(std::string_view name) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForFifo(), name);
    if (mkfifo(full_path.c_str(), __FAAS_FILE_CREAT_MODE) != 0) {
        PLOG(ERROR) << "mkfifo " << full_path << " failed";
        return false;
    }
    return true;
}

void FifoRemove(std::string_view name) {
    std::string full_path = fs_utils::JoinPath(GetRootPathForFifo(), name);
    if (!fs_utils::Remove(full_path)) {
        PLOG(ERROR) << "Failed to remove " << full_path;
    }
}

int FifoOpenForRead(std::string_view name, bool nonblocking) {
    return fs_utils::Open(
        fs_utils::JoinPath(GetRootPathForFifo(), name),
        /* flags= */ O_RDONLY | (nonblocking ? O_NONBLOCK : 0));
}

int FifoOpenForWrite(std::string_view name, bool nonblocking) {
    return fs_utils::Open(
        fs_utils::JoinPath(GetRootPathForFifo(), name),
        /* flags= */ O_WRONLY | (nonblocking ? O_NONBLOCK : 0));
}

int FifoOpenForReadWrite(std::string_view name, bool nonblocking) {
    return fs_utils::Open(
        fs_utils::JoinPath(GetRootPathForFifo(), name),
        /* flags= */ O_RDWR | (nonblocking ? O_NONBLOCK : 0));
}

}  // namespace ipc
}  // namespace faas
