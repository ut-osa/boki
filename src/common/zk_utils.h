#pragma once

#ifndef __FAAS_SRC
#error common/zk_utils.h cannot be included outside
#endif

#include "base/common.h"
#include "common/zk.h"

namespace faas {
namespace zk_utils {

// Helper functions
std::string_view EventName(zk::ZKEvent event);
bool ParseSequenceNumber(std::string_view created_path, uint64_t* parsed);

// Synchronous helper APIs
// They cannot be called from callbacks of ZKSession's asynchronous APIs
//
// Synchronously create a new node
zk::ZKStatus CreateSync(zk::ZKSession* session, std::string_view path,
                        std::span<const char> value,
                        zk::ZKCreateMode mode, std::string* created_path);
// Synchronously delete a node.
zk::ZKStatus DeleteSync(zk::ZKSession* session, std::string_view path);
// Get the value of `path`
// If `path` not exists, will wait for its creation
zk::ZKStatus GetOrWaitSync(zk::ZKSession* session, std::string_view path,
                           std::string* value);

class DirWatcher {
public:
    // The directory being watched MUST be flat
    DirWatcher(zk::ZKSession* session, std::string_view directory);
    ~DirWatcher();

    typedef std::function<void(std::string_view /* path */,
                               std::span<const char> /* contents */)>
            NodeCreatedCallback;
    void SetNodeCreatedCallback(NodeCreatedCallback cb);

    typedef std::function<void(std::string_view /* path */,
                               std::span<const char> /* contents */)>
            NodeChangedCallback;
    void SetNodeChangedCallback(NodeChangedCallback cb);

    typedef std::function<void(std::string_view /* path */)>
            NodeDeletedCallback;
    void SetNodeDeletedCallback(NodeDeletedCallback cb);

    void Start();

private:
    zk::ZKSession* session_;
    std::string dir_full_path_;

    absl::flat_hash_set</* node_path */ std::string> nodes_;

    NodeCreatedCallback node_created_cb_;
    NodeChangedCallback node_changed_cb_;
    NodeDeletedCallback node_deleted_cb_;

    std::string GetNodePath(std::string_view full_path) {
        std::string_view base = absl::StripPrefix(full_path, dir_full_path_);
        return std::string(absl::StripPrefix(base, "/"));
    }

    void InvokeGet(std::string path, bool from_get_watch);
    void InvokeGetChildren();

    void GetWatchCallback(zk::ZKEvent event, std::string_view path);
    void GetCallback(std::string path, bool from_get_watch,
                     zk::ZKStatus status,
                     const zk::ZKResult& result, bool* remove_watch);
    void GetChildrenWatchCallback(zk::ZKEvent event, std::string_view path);
    void GetChildrenCallback(zk::ZKStatus status,
                             const zk::ZKResult& result, bool* remove_watch);

    DISALLOW_COPY_AND_ASSIGN(DirWatcher);
};

}  // namespace zk_utils
}  // namespace faas
