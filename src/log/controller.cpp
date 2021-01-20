#include "log/controller.h"

#include "common/flags.h"

#define log_header_ "Controller: "

namespace faas {
namespace log {

using server::NodeWatcher;

Controller::Controller()
    : metalog_replicas_(kDefaultNumReplicas),
      userlog_replicas_(kDefaultNumReplicas),
      index_replicas_(kDefaultNumReplicas),
      state_(kCreated),
      zk_session_(absl::GetFlag(FLAGS_zookeeper_host),
                  absl::GetFlag(FLAGS_zookeeper_root_path)) {}

Controller::~Controller() {}

void Controller::Start() {
    zk_session_.Start();
    // Setup NodeWatcher
    node_watcher_.SetNodeOnlineCallback(
        absl::bind_front(&Controller::OnNodeOnline, this));
    node_watcher_.SetNodeOfflineCallback(
        absl::bind_front(&Controller::OnNodeOffline, this));
    node_watcher_.StartWatching(&zk_session_);
    // Setup command watcher
    cmd_watcher_.reset(new zk_utils::DirWatcher(&zk_session_, "cmd"));
    cmd_watcher_->SetNodeCreatedCallback(
        absl::bind_front(&Controller::OnCmdZNodeCreated, this));
    cmd_watcher_->Start();
}

void Controller::ScheduleStop() {
    zk_session_.ScheduleStop();
}

void Controller::WaitForFinish() {
    zk_session_.WaitForFinish();
}

void Controller::InstallNewView(const ViewProto& view_proto) {
    DCHECK_EQ(gsl::narrow_cast<uint16_t>(view_proto.view_id()),
              next_view_id());
    View* view = new View(view_proto);
    views_.emplace_back(view);
    std::string serialized;
    CHECK(view_proto.SerializeToString(&serialized));
    zk_session_.Create(
        "view/new", STRING_AS_SPAN(serialized),
        zk::ZKCreateMode::kPersistentSequential,
        [view] (zk::ZKStatus status, const zk::ZKResult& result, bool*) {
            if (!status.ok()) {
                HLOG(FATAL) << "Failed to publish the new view: " << status.ToString();
            }
            HLOG(INFO) << fmt::format("View {} is published as {}",
                                      view->id(), result.path);
        }
    );
}

void Controller::OnNodeOnline(NodeWatcher::NodeType node_type, uint16_t node_id) {
    switch (node_type) {
    case NodeWatcher::kSequencerNode:
        sequencer_nodes_.insert(node_id);
        break;
    case NodeWatcher::kEngineNode:
        engine_nodes_.insert(node_id);
        break;
    case NodeWatcher::kStorageNode:
        storage_nodes_.insert(node_id);
        break;
    default:
        break;
    }
}

void Controller::OnNodeOffline(NodeWatcher::NodeType node_type, uint16_t node_id) {
    switch (node_type) {
    case NodeWatcher::kSequencerNode:
        sequencer_nodes_.erase(node_id);
        break;
    case NodeWatcher::kEngineNode:
        engine_nodes_.erase(node_id);
        break;
    case NodeWatcher::kStorageNode:
        storage_nodes_.erase(node_id);
        break;
    default:
        break;
    }
}

void Controller::OnCmdZNodeCreated(std::string_view path,
                                   std::span<const char> contents) {
    if (path == "start") {
        StartCommandHandler();
    } else {
        HLOG(ERROR) << "Unknown command: " << path;
    }
    zk_session_.Delete(fmt::format("cmd/{}", path), nullptr);
}

void Controller::StartCommandHandler() {
    if (state_ != kCreated) {
        HLOG(WARNING) << "Already started";
        return;
    }
    CHECK_GT(metalog_replicas_, 0U);
    if (sequencer_nodes_.size() < metalog_replicas_) {
        HLOG(ERROR) << "Sequencer nodes not enough";
        return;
    }
    CHECK_GT(index_replicas_, 0U);
    if (engine_nodes_.size() < index_replicas_) {
        HLOG(ERROR) << "Engine nodes not enough";
        return;
    }
    CHECK_GT(userlog_replicas_, 0U);
    if (storage_nodes_.size() < userlog_replicas_) {
        HLOG(ERROR) << "Storage nodes not enough";
        return;
    }
    ViewProto view_proto;
    view_proto.set_view_id(0);
    view_proto.set_metalog_replicas(metalog_replicas_);
    view_proto.set_userlog_replicas(userlog_replicas_);
    view_proto.set_index_replicas(index_replicas_);
    for (uint16_t node_id : sequencer_nodes_) {
        view_proto.add_sequencer_nodes(node_id);
    }
    for (uint16_t node_id : engine_nodes_) {
        view_proto.add_engine_nodes(node_id);
    }
    for (uint16_t node_id : storage_nodes_) {
        view_proto.add_storage_nodes(node_id);
    }

    HLOG(INFO) << fmt::format("Create initial view with {} sequencers, {} engines, "
                              "and {} storages",
                              view_proto.sequencer_nodes_size(),
                              view_proto.engine_nodes_size(),
                              view_proto.storage_nodes_size());

    // Now all maps to the first sequencer
    // TODO: implement hashing properly
    view_proto.set_log_space_hash_seed(0);
    view_proto.add_log_space_hash_tokens(view_proto.sequencer_nodes(0));

    for (size_t i = 0; i < engine_nodes_.size() * userlog_replicas_; i++) {
        view_proto.add_storage_plan(
            view_proto.storage_nodes(i % storage_nodes_.size()));
    }
    for (size_t i = 0; i < sequencer_nodes_.size() * index_replicas_; i++) {
        view_proto.add_index_plan(
            view_proto.engine_nodes(i % engine_nodes_.size()));
    }

    InstallNewView(view_proto);
}

}  // namespace log
}  // namespace faas
