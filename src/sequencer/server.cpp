#include "sequencer/server.h"

#include "utils/io.h"
#include "utils/fs.h"

#define HLOG(l) LOG(l) << "Server: "
#define HVLOG(l) VLOG(l) << "Server: "

namespace faas {
namespace sequencer {

using protocol::SequencerMessage;
using protocol::SequencerMessageHelper;

Server::Server(uint16_t sequencer_id)
    : state_(kCreated),
      my_sequencer_id_(sequencer_id),
      event_loop_thread_("Server/EL", absl::bind_front(&Server::EventLoopThreadMain, this)),
      node_manager_(this),
      raft_(sequencer_id),
      core_(sequencer_id),
      global_cut_timerfd_(io_utils::CreateTimerFd()) {
    UV_CHECK_OK(uv_loop_init(&uv_loop_));
    uv_loop_.data = &event_loop_thread_;
    UV_CHECK_OK(uv_async_init(&uv_loop_, &stop_event_, &Server::StopCallback));
    stop_event_.data = this;
    CHECK(global_cut_timerfd_ != -1);
    UV_CHECK_OK(uv_poll_init(&uv_loop_, &global_cut_timer_, global_cut_timerfd_));
    global_cut_timer_.data = this;
    // Setup callbacks for SequencerCore
    core_.SetRaftLeaderCallback([this] (uint16_t* leader_id) -> bool {
        uint64_t id = raft_.GetLeader();
        if (id == 0) {
            return false;
        }
        *leader_id = gsl::narrow_cast<uint16_t>(id);
        return true;
    });
    core_.SetRaftApplyCallback([this] (uint32_t seqnum, std::span<const char> payload) {
        raft_.Apply(payload, [this, seqnum] (bool success) {
            core_.OnRaftApplyFinished(seqnum, success);
        });
    });
    core_.SetSendFsmRecordsMessageCallback(
        absl::bind_front(&Server::SendFsmRecordsMessage, this));
    // Setup callbacks for Raft
    raft_.SetFsmApplyCallback(
        absl::bind_front(&log::SequencerCore::RaftFsmApplyCallback, &core_));
    raft_.SetFsmRestoreCallback(
        absl::bind_front(&log::SequencerCore::RaftFsmRestoreCallback, &core_));
    raft_.SetFsmSnapshotCallback(
        absl::bind_front(&log::SequencerCore::RaftFsmSnapshotCallback, &core_));
}

Server::~Server() {
    State state = state_.load();
    DCHECK(state == kCreated || state == kStopped);
    UV_CHECK_OK(uv_loop_close(&uv_loop_));
}

void Server::Start() {
    DCHECK(state_.load() == kCreated);
    CHECK(config_.LoadFromFile(config_path_)) << "Failed to load sequencer config";
    const SequencerConfig::Peer* myself = config_.GetPeer(my_sequencer_id_);
    if (myself == nullptr) {
        HLOG(FATAL) << "Cannot find myself in the sequencer config";
    }
    node_manager_.Start(&uv_loop_, address_, myself->engine_conn_port);
    std::string raft_addr(fmt::format("{}:{}", address_, myself->raft_port));
    std::vector<std::pair<uint64_t, std::string>> peers;
    config_.ForEachPeer([&peers] (const SequencerConfig::Peer* peer) {
        peers.push_back(std::make_pair(uint64_t{peer->id},
                                       fmt::format("{}:{}", peer->host_addr, peer->raft_port)));
    });
    if (fs_utils::IsDirectory(raft_data_dir_)) {
        PCHECK(fs_utils::RemoveDirectoryRecursively(raft_data_dir_));
    }
    PCHECK(fs_utils::MakeDirectory(raft_data_dir_));
    raft_.Start(&uv_loop_, raft_addr, raft_data_dir_, peers);
    CHECK(io_utils::SetupTimerFd(global_cut_timerfd_, 0, core_.global_cut_interval_us()));
    UV_CHECK_OK(uv_poll_start(&global_cut_timer_, UV_READABLE, &Server::GlobalCutTimerCallback));
    // Start thread for running event loop
    event_loop_thread_.Start();
    state_.store(kRunning);
}

void Server::ScheduleStop() {
    HLOG(INFO) << "Scheduled to stop";
    UV_CHECK_OK(uv_async_send(&stop_event_));
}

void Server::WaitForFinish() {
    DCHECK(state_.load() != kCreated);
    event_loop_thread_.Join();
    DCHECK(state_.load() == kStopped);
    HLOG(INFO) << "Stopped";
}

void Server::EventLoopThreadMain() {
    HLOG(INFO) << "Event loop starts";
    int ret = uv_run(&uv_loop_, UV_RUN_DEFAULT);
    if (ret != 0) {
        HLOG(WARNING) << "uv_run returns non-zero value: " << ret;
    }
    HLOG(INFO) << "Event loop finishes";
    state_.store(kStopped);
}

void Server::OnNewNodeConnected(uint16_t node_id, std::string_view shared_log_addr) {
    core_.OnNewNodeConnected(node_id, shared_log_addr);
}

void Server::OnNodeDisconnected(uint16_t node_id) {
    core_.OnNodeDisconnected(node_id);
}

void Server::OnRecvNodeMessage(uint16_t node_id, const SequencerMessage& message,
                               std::span<const char> payload) {
    if (SequencerMessageHelper::IsLocalCut(message)) {
        HVLOG(1) << fmt::format("Receive local cut message from node {}", node_id);
        log::LocalCutMsgProto message_proto;
        if (!message_proto.ParseFromArray(payload.data(), payload.size())) {
            HLOG(ERROR) << "Failed to parse sequencer message!";
            return;
        }
        core_.OnRecvLocalCutMessage(message_proto);
    } else {
        HLOG(ERROR) << fmt::format("Unknown message type: {}!", message.message_type);
    }
}

void Server::SendFsmRecordsMessage(uint16_t node_id, std::span<const char> data) {
    SequencerMessage message = SequencerMessageHelper::NewFsmRecords(data);
    if (!node_manager_.SendMessage(node_id, message, data)) {
        HLOG(ERROR) << fmt::format("Failed to send FsmRecordsMessage to node {}", node_id);
    }
}

UV_POLL_CB_FOR_CLASS(Server, GlobalCutTimer) {
    core_.MarkGlobalCutIfNeeded();
}

UV_ASYNC_CB_FOR_CLASS(Server, Stop) {
    if (state_.load(std::memory_order_consume) == kStopping) {
        HLOG(WARNING) << "Already in stopping state";
        return;
    }
    HLOG(INFO) << "Start stopping process";
    node_manager_.ScheduleStop();
    raft_.ScheduleClose();
    uv_close(UV_AS_HANDLE(&stop_event_), nullptr);
    uv_close(UV_AS_HANDLE(&global_cut_timer_), nullptr);
    state_.store(kStopping);
}

}  // namespace sequencer
}  // namespace faas
