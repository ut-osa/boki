#define __FAAS_USED_IN_BINDING
#include "utils/fs.h"

#include <fcntl.h>
#include <ftw.h>

namespace faas {
namespace fs_utils {

namespace {
bool Stat(std::string_view path, struct stat* statbuf) {
    return stat(std::string(path).c_str(), statbuf) == 0;
}

int RemoveFileFtwFn(const char* fpath, const struct stat* sb,
                    int typeflag, struct FTW *ftwbuf) {
    return remove(fpath) != 0;
}
}

bool Exists(std::string_view path) {
    return access(std::string(path).c_str(), F_OK) == 0;
}

bool IsFile(std::string_view path) {
    struct stat statbuf;
    if (!Stat(path, &statbuf)) {
        return false;
    }
    return S_ISREG(statbuf.st_mode) != 0;
}

bool IsDirectory(std::string_view path) {
    struct stat statbuf;
    if (!Stat(path, &statbuf)) {
        return false;
    }
    return S_ISDIR(statbuf.st_mode) != 0;
}

std::string GetRealPath(std::string_view path) {
    char* result = realpath(std::string(path).c_str(), nullptr);
    if (result == nullptr) {
        LOG(WARNING) << path << " is not a valid path";
        return std::string(path);
    }
    std::string result_str(result);
    free(result);
    return result_str;
}

bool MakeDirectory(std::string_view path) {
    return mkdir(std::string(path).c_str(), __FAAS_DIR_CREAT_MODE) == 0;
}

bool Remove(std::string_view path) {
    return remove(std::string(path).c_str()) == 0;
}

bool RemoveDirectoryRecursively(std::string_view path) {
    return nftw(std::string(path).c_str(), RemoveFileFtwFn, 8,
                FTW_DEPTH|FTW_MOUNT|FTW_PHYS) == 0;
}

bool ReadContents(std::string_view path, std::string* contents) {
    FILE* fin = fopen(std::string(path).c_str(), "rb");
    if (fin == nullptr) {
        LOG(ERROR) << "Failed to open file: " << path;
        return false;
    }
    auto close_file = gsl::finally([fin] { fclose(fin); });
    char buffer[128];
    contents->clear();
    while (feof(fin) == 0) {
        size_t nread = fread(buffer, 1, sizeof(buffer), fin);
        if (nread > 0) {
            contents->append(buffer, nread);
        } else {
            break;
        }
    }
    return true;
}

bool ReadLink(std::string_view path, std::string* contents) {
    std::string buf;
    buf.resize(128);
    while (buf.size() < 4096) {
        ssize_t ret = readlink(std::string(path).c_str(), buf.data(), buf.size());
        if (ret < 0) {
            PLOG_F(WARNING, "readlink failed for {}", path);
            return false;
        }
        size_t nread = gsl::narrow_cast<size_t>(ret);
        if (nread == 0) {
            LOG_F(ERROR, "readlink returns empty for {}", path);
            return false;
        }
        if (nread < buf.size()) {
            contents->assign(buf.data(), nread);
            return true;
        }
        buf.resize(buf.size() * 2);
    }
    return false;
}

std::optional<int> Open(std::string_view full_path, int flags) {
    int fd = open(std::string(full_path).c_str(), flags | O_CLOEXEC);
    if (fd == -1) {
        PLOG_F(ERROR, "Open {} failed", full_path);
        return std::nullopt;
    }
    return fd;
}

std::optional<int> Create(std::string_view full_path) {
    int fd = open(
        std::string(full_path).c_str(),
        /* flags= */ O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC,
        /* mode=  */ __FAAS_FILE_CREAT_MODE);
    if (fd == -1) {
        PLOG_F(ERROR, "Create {} failed", full_path);
        return std::nullopt;
    }
    return fd;
}

std::string JoinPath(std::string_view path1, std::string_view path2) {
    return fmt::format("{}/{}", path1, path2);
}

std::string JoinPath(std::string_view path1, std::string_view path2, std::string_view path3) {
    return fmt::format("{}/{}/{}", path1, path2, path3);
}

}  // namespace fs_utils
}  // namespace faas
