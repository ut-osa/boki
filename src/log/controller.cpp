#include "log/controller.h"

#include "log/flags.h"
#include "utils/random.h"
#include "utils/bits.h"
#include "utils/hash.h"

#define log_header_ "Controller: "

namespace faas {
namespace log {

using server::NodeWatcher;

Controller::Controller(uint32_t random_seed)
    : rnd_gen_(random_seed),
      metalog_replicas_(kDefaultNumReplicas),
      userlog_replicas_(kDefaultNumReplicas),
      index_replicas_(kDefaultNumReplicas),
      state_(kCreated),
      zk_session_(absl::GetFlag(FLAGS_zookeeper_host),
                  absl::GetFlag(FLAGS_zookeeper_root_path)) {
    LOG_F(INFO, "Random seed is {}", bits::HexStr0x(random_seed));
}

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
    cmd_watcher_.emplace(&zk_session_, "cmd");
    cmd_watcher_->SetNodeCreatedCallback(
        absl::bind_front(&Controller::OnCmdZNodeCreated, this));
    cmd_watcher_->Start();
    // Setup freeze watcher
    freeze_watcher_.emplace(&zk_session_, "freeze",
                            /* sequential_znodes= */ true);
    freeze_watcher_->SetNodeCreatedCallback(
        absl::bind_front(&Controller::OnFreezeZNodeCreated, this));
    freeze_watcher_->Start();
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
            HLOG_F(INFO, "View {} is published as {}", view->id(), result.path);
        }
    );
    state_ = kNormal;
}

void Controller::ReconfigView(const Configuration& configuration) {
    if (configuration.sequencer_nodes.size() < metalog_replicas_
          || configuration.sequencer_nodes.size() < num_phylogs_) {
        HLOG(ERROR) << "Sequencer nodes not enough";
        return;
    }
    if (configuration.engine_nodes.size() < index_replicas_) {
        HLOG(ERROR) << "Engine nodes not enough";
        return;
    }
    if (configuration.storage_nodes.size() < userlog_replicas_) {
        HLOG(ERROR) << "Storage nodes not enough";
        return;
    }

    if (state_ == kNormal) {
        DCHECK(!views_.empty());
        DCHECK(!pending_reconfig_.has_value());
        pending_reconfig_ = configuration;
        FreezeView(current_view());
        return;
    }

    if (state_ == kReconfiguring) {
        HLOG(ERROR) << "A reconfiguration is ongoing";
        return;
    }

    ViewProto view_proto;
    view_proto.set_view_id(next_view_id());
    view_proto.set_metalog_replicas(gsl::narrow_cast<uint32_t>(metalog_replicas_));
    view_proto.set_userlog_replicas(gsl::narrow_cast<uint32_t>(userlog_replicas_));
    view_proto.set_index_replicas(gsl::narrow_cast<uint32_t>(index_replicas_));
    view_proto.set_num_phylogs(gsl::narrow_cast<uint32_t>(configuration.num_phylogs));
    for (uint16_t node_id : configuration.sequencer_nodes) {
        view_proto.add_sequencer_nodes(node_id);
    }
    for (uint16_t node_id : configuration.engine_nodes) {
        view_proto.add_engine_nodes(node_id);
    }
    for (uint16_t node_id : configuration.storage_nodes) {
        view_proto.add_storage_nodes(node_id);
    }

    view_proto.set_log_space_hash_seed(configuration.log_space_hash_seed);
    for (uint32_t token : configuration.log_space_hash_tokens) {
        view_proto.add_log_space_hash_tokens(token);
    }

    size_t num_sequencers = configuration.sequencer_nodes.size();
    size_t num_engines = configuration.engine_nodes.size();
    size_t num_storages = configuration.storage_nodes.size();

    for (size_t i = 0; i < num_engines * userlog_replicas_; i++) {
        view_proto.add_storage_plan(configuration.storage_nodes.at(i % num_storages));
    }
    for (size_t i = 0; i < num_sequencers * index_replicas_; i++) {
        view_proto.add_index_plan(configuration.engine_nodes.at(i % num_engines));
    }

    InstallNewView(view_proto);
}

void Controller::FreezeView(const View* view) {
    HLOG_F(INFO, "Start sealing for view {}", view->id());
    OngoingSeal seal;
    seal.view = view;
    seal.phylogs.clear();
    for (uint16_t sequencer_id : view->GetSequencerNodes()) {
        if (view->is_active_phylog(sequencer_id)) {
            seal.phylogs.push_back(sequencer_id);
        }
    }
    seal.tail_metalogs.clear();
    ongoing_seal_ = seal;

    std::string data = fmt::format("{}", view->id());
    zk_session_.Create(
        "view/freeze", STRING_AS_SPAN(data),
        zk::ZKCreateMode::kPersistentSequential,
        [view] (zk::ZKStatus status, const zk::ZKResult& result, bool*) {
            if (!status.ok()) {
                HLOG(FATAL) << "Failed to freeze the view: " << status.ToString();
            }
            HLOG_F(INFO, "View {} freeze cmd is published as {}", view->id(), result.path);
        }
    );
    state_ = kReconfiguring;
}

namespace {
void BuildFinalTailMetalog(const std::vector<MetaLogsProto> tail_metalogs,
                           uint32_t* final_position, MetaLogsProto* final_tail) {
    std::map<uint32_t, MetaLogProto> entries;
    for (const MetaLogsProto& metalogs : tail_metalogs) {
        DCHECK_EQ(metalogs.logspace_id(), final_tail->logspace_id());
        for (const MetaLogProto& metalog : metalogs.metalogs()) {
            entries[metalog.metalog_seqnum()] = metalog;
        }
    }
    *final_position = 0;
    for (const auto& [metalog_seqnum, metalog] : entries) {
        final_tail->add_metalogs()->CopyFrom(metalog);
        *final_position = metalog_seqnum + 1;
    }
}
}  // namespace

std::optional<FinalizedViewProto> Controller::CheckAllSealed(const OngoingSeal& seal) {
    FinalizedViewProto finalized_view_proto;
    finalized_view_proto.set_view_id(seal.view->id());
    for (uint16_t sequencer_id : seal.phylogs) {
        std::vector<MetaLogsProto> collected;
        for (const auto& [key, value] : seal.tail_metalogs) {
            if (key.first == sequencer_id) {
                collected.push_back(value);
            }
        }
        size_t quorum = (seal.view->metalog_replicas() + 1) / 2;
        if (collected.size() < quorum) {
            return std::nullopt;
        }
        uint32_t final_position;
        MetaLogsProto* final_tail = finalized_view_proto.add_tail_metalogs();
        final_tail->set_logspace_id(bits::JoinTwo16(seal.view->id(), sequencer_id));
        BuildFinalTailMetalog(collected, &final_position, final_tail);
        finalized_view_proto.add_metalog_positions(final_position);
    }
    return finalized_view_proto;
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
    } else if (path == "info") {
        InfoCommandHandler();
    } else if (path == "reconfig") {
        ReconfigCommandHandler(std::string(contents.data(), contents.size()));
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
    CHECK_GT(num_phylogs_, 0U);
    HLOG_F(INFO, "Number of physical logs: {}", num_phylogs_);
    CHECK_GT(index_replicas_, 0U);
    CHECK_GT(userlog_replicas_, 0U);

    NodeIdVec sequencer_nodes(sequencer_nodes_.begin(), sequencer_nodes_.end());
    NodeIdVec engine_nodes(engine_nodes_.begin(), engine_nodes_.end());
    NodeIdVec storage_nodes(storage_nodes_.begin(), storage_nodes_.end());

    log_space_hash_seed_ = hash::xxHash64(rnd_gen_());
    log_space_hash_tokens_.resize(absl::GetFlag(FLAGS_slog_log_space_hash_tokens));
    for (size_t i = 0; i < log_space_hash_tokens_.size(); i++) {
        log_space_hash_tokens_[i] = sequencer_nodes.at(i % num_phylogs_);
    }
    std::shuffle(log_space_hash_tokens_.begin(),
                 log_space_hash_tokens_.end(),
                 rnd_gen_);

    HLOG_F(INFO, "Create initial view with {} sequencers, {} engines, and {} storages",
           sequencer_nodes.size(), engine_nodes.size(), storage_nodes.size());
    Configuration configuration = {
        .log_space_hash_seed   = log_space_hash_seed_,
        .log_space_hash_tokens = log_space_hash_tokens_,
        .num_phylogs     = num_phylogs_,
        .sequencer_nodes = std::move(sequencer_nodes),
        .engine_nodes    = std::move(engine_nodes),
        .storage_nodes   = std::move(storage_nodes),
    };
    ReconfigView(configuration);
}

void Controller::InfoCommandHandler() {
    std::stringstream stream;

    stream << "Current state: ";
    switch (state_) {
    case kCreated:
        stream << "Created";
        break;
    case kNormal:
        stream << "Normal";
        break;
    case kReconfiguring:
        stream << "Reconfiguring";
        break;
    case kFrozen:
        stream << "Frozen";
        break;
    default:
        UNREACHABLE();
    }
    stream << "\n";

    if (!views_.empty()) {
        const View* view = current_view();
        stream << fmt::format("Current view[{}]:", view->id()) << "\n";
        stream << "  MetaLogReplica = " << view->metalog_replicas() << "\n";
        stream << "  UserLogReplica = " << view->userlog_replicas() << "\n";
        stream << "  IndexReplica = " << view->index_replicas() << "\n";
        stream << "  NumPhyLogs = " << view->num_phylogs() << "\n";
        stream << "  Sequencers = [";
        for (uint16_t sequencer_id : view->GetSequencerNodes()) {
            stream << sequencer_id << ", ";
        }
        stream << "]\n";
        stream << "  Engines = [";
        for (uint16_t engine_id : view->GetEngineNodes()) {
            stream << engine_id << ", ";
        }
        stream << "]\n";
        stream << "  Storages = [";
        for (uint16_t storage_id : view->GetStorageNodes()) {
            stream << storage_id << ", ";
        }
        stream << "]\n";
    }

    LOG(INFO) << "\n[START PRINTING INFO]\n"
              << stream.str()
              << "[END PRINTING INFO]";
}

namespace {
int ParseIntChecked(std::string_view s) {
    int parsed;
    if (!absl::SimpleAtoi(s, &parsed)) {
        LOG(FATAL) << "Failed to parse: " << s;
    }
    return parsed;
}
}  // namespace

void Controller::ReconfigCommandHandler(std::string inputs) {
    if (state_ != kNormal) {
        HLOG(ERROR) << "Not in normal state, cannot reconfigure";
        return;
    }

    const View* view = current_view();
    Configuration configuration;
    configuration.log_space_hash_seed = view->log_space_hash_seed();
    configuration.num_phylogs = view->num_phylogs();
    configuration.sequencer_nodes.assign(
        view->GetSequencerNodes().begin(),
        view->GetSequencerNodes().end());
    std::shuffle(configuration.sequencer_nodes.begin(),
                 configuration.sequencer_nodes.end(),
                 rnd_gen_);
    configuration.engine_nodes.assign(
        view->GetEngineNodes().begin(),
        view->GetEngineNodes().end());
    std::shuffle(configuration.engine_nodes.begin(),
                 configuration.engine_nodes.end(),
                 rnd_gen_);
    configuration.storage_nodes.assign(
        view->GetStorageNodes().begin(),
        view->GetStorageNodes().end());
    std::shuffle(configuration.storage_nodes.begin(),
                 configuration.storage_nodes.end(),
                 rnd_gen_);

    std::vector<std::string_view> parts = absl::StrSplit(inputs, ' ');
    if (parts[0] == "seq") {
        size_t origin_count = configuration.sequencer_nodes.size();
        configuration.sequencer_nodes.clear();
        for (size_t i = 1; i < parts.size(); i++) {
            if (!parts[i].empty()) {
                configuration.sequencer_nodes.push_back(
                    gsl::narrow_cast<uint16_t>(ParseIntChecked(parts[i])));
            }
        }
        if (configuration.sequencer_nodes.size() != origin_count) {
            HLOG_F(FATAL, "The numbers of sequencers mismatch: given={}, expect={}",
                   configuration.sequencer_nodes.size(), origin_count);
        }
    }

    for (size_t i = 0; i < view->log_space_hash_tokens().size(); i++) {
        configuration.log_space_hash_tokens.push_back(configuration.sequencer_nodes.at(
            i % configuration.num_phylogs));
    }
    std::shuffle(configuration.log_space_hash_tokens.begin(),
                 configuration.log_space_hash_tokens.end(),
                 rnd_gen_);

    ReconfigView(configuration);
}

void Controller::OnFreezeZNodeCreated(std::string_view path,
                                      std::span<const char> contents) {
    if (!ongoing_seal_.has_value()) {
        return;
    }
    const View* view = ongoing_seal_->view;
    FrozenSequencerProto frozen_proto;
    if (!frozen_proto.ParseFromArray(contents.data(),
                                     static_cast<int>(contents.size()))) {
        HLOG(FATAL) << "Failed to parse FrozenSequencerProto";
    }
    if (frozen_proto.view_id() != view->id()) {
        return;
    }
    HLOG_F(INFO, "Receive seal response from sequencer {} for view {}",
           frozen_proto.sequencer_id(), view->id());
    for (const auto& tail_metalogs : frozen_proto.tail_metalogs()) {
        auto key = std::make_pair(tail_metalogs.logspace_id(), frozen_proto.sequencer_id());
        ongoing_seal_->tail_metalogs[key] = tail_metalogs;
    }
    auto sealed = CheckAllSealed(*ongoing_seal_);
    if (!sealed.has_value()) {
        return;
    }
    ongoing_seal_.reset();
    HLOG_F(INFO, "Finish sealing for view {}", view->id());
    FinalizedViewProto finalized_view = *sealed;
    std::string serialized;
    CHECK(finalized_view.SerializeToString(&serialized));
    zk_session_.Create(
        "view/finalize", STRING_AS_SPAN(serialized),
        zk::ZKCreateMode::kPersistentSequential,
        [view, this] (zk::ZKStatus status, const zk::ZKResult& result, bool*) {
            if (!status.ok()) {
                HLOG(FATAL) << "Failed to publish the new finalized view: " << status.ToString();
            }
            HLOG_F(INFO, "Finalized view {} is published as {}", view->id(), result.path);
            state_ = kFrozen;
            Configuration configuration = *pending_reconfig_;
            pending_reconfig_.reset();
            ReconfigView(configuration);
        }
    );
}

}  // namespace log
}  // namespace faas
