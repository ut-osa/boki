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
    enum State { kCreated, kNormal, kReconfiguring, kFrozen };

    std::mt19937 rnd_gen_;

    size_t metalog_replicas_;
    size_t userlog_replicas_;
    size_t index_replicas_;
    size_t num_phylogs_;

    State state_;

    zk::ZKSession zk_session_;
    server::NodeWatcher node_watcher_;
    std::optional<zk_utils::DirWatcher> cmd_watcher_;
    std::optional<zk_utils::DirWatcher> freeze_watcher_;

    uint64_t log_space_hash_seed_;
    std::vector<uint32_t> log_space_hash_tokens_;

    std::set</* node_id */ uint16_t> sequencer_nodes_;
    std::set</* node_id */ uint16_t> engine_nodes_;
    std::set</* node_id */ uint16_t> storage_nodes_;

    std::vector<std::unique_ptr<View>> views_;

    using NodeIdVec = std::vector<uint16_t>;
    struct Configuration {
        uint64_t              log_space_hash_seed;
        std::vector<uint32_t> log_space_hash_tokens;

        size_t    num_phylogs;
        NodeIdVec sequencer_nodes;
        NodeIdVec engine_nodes;
        NodeIdVec storage_nodes;
    };
    std::optional<Configuration> pending_reconfig_;

    struct OngoingSeal {
        const View* view;
        std::vector<uint16_t> phylogs;
        absl::flat_hash_map<std::pair<uint16_t, uint16_t>,
                            MetaLogsProto>
            tail_metalogs;
    };
    std::optional<OngoingSeal> ongoing_seal_;

    inline uint16_t next_view_id() const {
        return gsl::narrow_cast<uint16_t>(views_.size());
    }
    inline const View* current_view() const {
        return views_.empty() ? nullptr : views_.back().get();
    }

    void InstallNewView(const ViewProto& view_proto);
    void ReconfigView(const Configuration& configuration);
    void FreezeView(const View* view);

    std::optional<FinalizedViewProto> CheckAllSealed(const OngoingSeal& seal);

    void OnNodeOnline(server::NodeWatcher::NodeType node_type, uint16_t node_id);
    void OnNodeOffline(server::NodeWatcher::NodeType node_type, uint16_t node_id);

    void OnCmdZNodeCreated(std::string_view path, std::span<const char> contents);
    void OnFreezeZNodeCreated(std::string_view path, std::span<const char> contents);

    void StartCommandHandler();
    void InfoCommandHandler();
    void ReconfigCommandHandler(std::string inputs);

    DISALLOW_COPY_AND_ASSIGN(Controller);
};

}  // namespace log
}  // namespace faas
