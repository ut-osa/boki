#pragma once

#include "base/common.h"
#include "common/zk.h"
#include "common/zk_utils.h"
#include "server/node_watcher.h"
#include "log/view.h"

namespace faas {
namespace log {

class Controller {
public:
    static constexpr size_t kDefaultNumReplicas = 3;

    Controller();
    ~Controller();

    void set_metalog_replicas(size_t value) { metalog_replicas_ = value; }
    void set_userlog_replicas(size_t value) { userlog_replicas_ = value; }
    void set_index_replicas(size_t value) { index_replicas_ = value; }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

private:
    enum State { kCreated, kViewActive, kViewFrozen };

    size_t metalog_replicas_;
    size_t userlog_replicas_;
    size_t index_replicas_;

    State state_;

    zk::ZKSession zk_session_;
    server::NodeWatcher node_watcher_;
    std::unique_ptr<zk_utils::DirWatcher> cmd_watcher_;

    absl::flat_hash_set</* node_id */ uint16_t> sequencer_nodes_;
    absl::flat_hash_set</* node_id */ uint16_t> engine_nodes_;
    absl::flat_hash_set</* node_id */ uint16_t> storage_nodes_;

    std::vector<std::unique_ptr<View>> views_;

    inline uint16_t next_view_id() const {
        return gsl::narrow_cast<uint16_t>(views_.size());
    }

    void InstallNewView(const ViewProto& view_proto);

    void OnNodeOnline(server::NodeWatcher::NodeType node_type, uint16_t node_id);
    void OnNodeOffline(server::NodeWatcher::NodeType node_type, uint16_t node_id);

    void OnCmdZNodeCreated(std::string_view path, std::span<const char> contents);

    void StartCommandHandler();

    DISALLOW_COPY_AND_ASSIGN(Controller);
};

}  // namespace log
}  // namespace faas
