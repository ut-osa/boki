#pragma once

#include "base/common.h"
#include "common/zk.h"
#include "common/zk_utils.h"
#include "server/node_watcher.h"
#include "log/view.h"

#include <random>

namespace faas {
namespace log {

class Controller {
public:
    static constexpr size_t kDefaultNumReplicas = 3;

    explicit Controller(uint32_t random_seed);
    ~Controller();

    void set_metalog_replicas(size_t value) { metalog_replicas_ = value; }
    void set_userlog_replicas(size_t value) { userlog_replicas_ = value; }
    void set_index_replicas(size_t value) { index_replicas_ = value; }
    void set_num_phylogs(size_t value) { num_phylogs_ = value; }

    void Start();
    void ScheduleStop();
    void WaitForFinish();

private:
    enum State { kCreated, kViewActive, kViewFrozen };

    std::mt19937 rnd_gen_;

    size_t metalog_replicas_;
    size_t userlog_replicas_;
    size_t index_replicas_;
    size_t num_phylogs_;

    State state_;

    zk::ZKSession zk_session_;
    server::NodeWatcher node_watcher_;
    std::optional<zk_utils::DirWatcher> cmd_watcher_;

    uint64_t log_space_hash_seed_;
    std::vector<uint32_t> log_space_hash_tokens_;

    std::set</* node_id */ uint16_t> sequencer_nodes_;
    std::set</* node_id */ uint16_t> engine_nodes_;
    std::set</* node_id */ uint16_t> storage_nodes_;

    std::vector<std::unique_ptr<View>> views_;

    inline uint16_t next_view_id() const {
        return gsl::narrow_cast<uint16_t>(views_.size());
    }

    void InstallNewView(const ViewProto& view_proto);

    using NodeIdVec = std::vector<uint16_t>;
    void ReconfigView(const NodeIdVec& sequencer_nodes,
                      const NodeIdVec& engine_nodes,
                      const NodeIdVec& storage_nodes);

    void OnNodeOnline(server::NodeWatcher::NodeType node_type, uint16_t node_id);
    void OnNodeOffline(server::NodeWatcher::NodeType node_type, uint16_t node_id);

    void OnCmdZNodeCreated(std::string_view path, std::span<const char> contents);

    void StartCommandHandler();

    DISALLOW_COPY_AND_ASSIGN(Controller);
};

}  // namespace log
}  // namespace faas
