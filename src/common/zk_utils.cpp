#include "common/zk_utils.h"

#include "utils/fs.h"

namespace faas {
namespace zk_utils {

using zk::ZKResult;
using zk::ZKStatus;
using zk::ZKEvent;
using zk::ZKCreateMode;
using zk::ZKSession;

std::string_view EventName(ZKEvent event) {
    int type = event.type;
    if (type == ZOO_CREATED_EVENT) {
        return "created";
    } else if (type == ZOO_DELETED_EVENT) {
        return "deleted";
    } else if (type == ZOO_CHANGED_EVENT) {
        return "changed";
    } else if (type == ZOO_CHILD_EVENT) {
        return "child";
    } else {
        UNREACHABLE();
    }
}

bool ParseSequenceNumber(std::string_view created_path, uint64_t* parsed) {
    static constexpr size_t kSequenceNumberLength = 10;
    size_t length = created_path.length();
    if (length < kSequenceNumberLength) {
        return false;
    }
    return absl::SimpleAtoi(created_path.substr(length - kSequenceNumberLength), parsed);
}

ZKStatus CreateSync(ZKSession* session, std::string_view path, std::span<const char> value,
                    ZKCreateMode mode, std::string* created_path) {
    ZKStatus ret_status;
    absl::Notification finished;
    session->Create(path, value, mode, [&] (ZKStatus status, const ZKResult& result, bool*) {
        if (status.ok() && created_path != nullptr) {
            *created_path = std::string(result.path);
        }
        ret_status = status;
        finished.Notify();
    });
    finished.WaitForNotification();
    DCHECK(!ret_status.undefined());
    return ret_status;
}

ZKStatus DeleteSync(ZKSession* session, std::string_view path) {
    ZKStatus ret_status;
    absl::Notification finished;
    session->Delete(path, [&] (ZKStatus status, const ZKResult& result, bool*) {
        ret_status = status;
        finished.Notify();
    });
    finished.WaitForNotification();
    DCHECK(!ret_status.undefined());
    return ret_status;
}

ZKStatus GetOrWaitSync(ZKSession* session, std::string_view path, std::string* value) {
    ZKStatus ret_status;
    absl::Notification finished;
    auto get_cb = [&] (ZKStatus status, const ZKResult& result, bool*) {
        if (status.ok() && value != nullptr) {
            value->assign(result.data.data(), result.data.size());
        }
        ret_status = status;
        finished.Notify();
    };
    auto exists_cb = [&] (ZKStatus status, const ZKResult&, bool* remove_watch) {
        if (status.ok()) {
            session->Get(path, nullptr, get_cb);
            *remove_watch = true;
        } else if (status.IsNoNode()) {
            VLOG(1) << "Node not exists, will rely on watch";
        } else {
            *remove_watch = true;
            ret_status = status;
            finished.Notify();
        }
    };
    auto watcher_fn = [&] (ZKEvent event, std::string_view) {
        if (event.IsCreated()) {
            session->Get(path, nullptr, get_cb);
        } else {
            LOG(ERROR) << "Receive watch event other than created: " << event.type;
            ret_status.code = ZNONODE;
            finished.Notify();
        }
    };
    session->Exists(path, watcher_fn, exists_cb);
    finished.WaitForNotification();
    DCHECK(!ret_status.undefined());
    return ret_status;
}

DirWatcher::DirWatcher(zk::ZKSession* session, std::string_view directory)
    : session_(session) {
    if (absl::StartsWith(directory, "/")) {
        dir_full_path_ = std::string(directory);
    } else {
        dir_full_path_ = fs_utils::JoinPath(session->root_path(), directory);
    }
}

DirWatcher::~DirWatcher() {
    if (session_->running()) {
        LOG(FATAL) << fmt::format("Watcher on directory {} is destructed "
                                  "while ZKSession still running",
                                   dir_full_path_);
    }
}

void DirWatcher::SetNodeCreatedCallback(NodeCreatedCallback cb) {
    node_created_cb_ = cb;
}

void DirWatcher::SetNodeChangedCallback(NodeChangedCallback cb) {
    node_changed_cb_ = cb;
}

void DirWatcher::SetNodeDeletedCallback(NodeDeletedCallback cb) {
    node_deleted_cb_ = cb;
}

void DirWatcher::Start() {
    InvokeGetChildren();
}

void DirWatcher::InvokeGet(std::string_view path, bool from_get_watch) {
    session_->Get(
        path,
        absl::bind_front(&DirWatcher::GetWatchCallback, this),
        absl::bind_front(&DirWatcher::GetCallback, this, path, from_get_watch));
}

void DirWatcher::InvokeGetChildren() {
    session_->GetChildren(
        dir_full_path_,
        absl::bind_front(&DirWatcher::GetChildrenWatchCallback, this),
        absl::bind_front(&DirWatcher::GetChildrenCallback, this));
}

void DirWatcher::GetWatchCallback(ZKEvent event, std::string_view path) {
    if (event.IsDeleted()) {
        std::string node_path = GetNodePath(path);
        if (nodes_.contains(node_path)) {
            if (node_deleted_cb_) {
                node_deleted_cb_(node_path);
            }
            nodes_.erase(node_path);
        }
    } else if (event.IsChanged()) {
        InvokeGet(path, true);
    } else {
        LOG(FATAL) << fmt::format("Get watch receives {} event", EventName(event));
    }
}

void DirWatcher::GetCallback(std::string_view path, bool from_get_watch,
                             ZKStatus status, const ZKResult& result, bool* remove_watch) {
    if (status.ok()) {
        std::string node_path = GetNodePath(path);
        if (!nodes_.contains(node_path)) {
            if (node_created_cb_) {
                node_created_cb_(node_path, result.data);
            }
            nodes_.insert(node_path);
        } else if (!from_get_watch) {
            VLOG(1) << fmt::format("Duplicate Get from GetChildrenCallback for path {}",
                                   node_path);
            *remove_watch = true;
        } else if (node_changed_cb_) {
            node_changed_cb_(node_path, result.data);
        }
    } else if (status.IsNoNode()) {
        std::string node_path = GetNodePath(path);
        VLOG(1) << fmt::format("NoNode path {}", node_path);
        if (nodes_.contains(node_path)) {
            if (node_deleted_cb_) {
                node_deleted_cb_(node_path);
            }
            nodes_.erase(node_path);
        }
        *remove_watch = true;
    } else {
        LOG(FATAL) << fmt::format("Get failed on path {}: {}", path, status.ToString());
    }
}

void DirWatcher::GetChildrenWatchCallback(ZKEvent event, std::string_view path) {
    if (event.IsChild()) {
        InvokeGetChildren();
    } else {
        LOG(FATAL) << fmt::format("GetChildren watch receives {} event",
                                  EventName(event));
    }
}

void DirWatcher::GetChildrenCallback(ZKStatus status, const ZKResult& result, bool*) {
    if (!status.ok()) {
        LOG(FATAL) << fmt::format("GetChildren failed on path {}: {}",
                                  dir_full_path_, status.ToString());
    }
    for (std::string_view path : result.paths) {
        std::string node_path(path);
        if (!nodes_.contains(node_path)) {
            VLOG(1) << fmt::format("Seen new node {} in GetChildren", node_path);
            InvokeGet(fs_utils::JoinPath(dir_full_path_, node_path), false);
        }
    }
}

}  // namespace zk_utils
}  // namespace faas
