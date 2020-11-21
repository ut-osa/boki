#include "common/sequencer_config.h"

#include "utils/fs.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace faas {

SequencerConfig::SequencerConfig() {}

SequencerConfig::~SequencerConfig() {}

bool SequencerConfig::Load(std::string_view json_contents) {
    json config;
    try {
        config = json::parse(json_contents);
    } catch (const json::parse_error& e) {
        LOG(ERROR) << "Failed to parse json: " << e.what();
        return false;
    }
    peers_.clear();
    try {
        if (!config.is_object()) {
            LOG(ERROR) << "Invalid config file";
            return false;
        }
        for (const auto& entry : config.items()) {
            int sequencer_id;
            if (!absl::SimpleAtoi(entry.key(), &sequencer_id)) {
                LOG(ERROR) << "Failed to parse sequencer ID";
                return false;
            }
            if (sequencer_id <= 0 || sequencer_id >= (1 << 16)) {
                LOG(ERROR) << fmt::format("Invalid sequencer ID: {}", sequencer_id);
                return false;
            }
            const auto& value = entry.value();
            if (!value.is_object()) {
                LOG(ERROR) << "Invalid config file";
                return false;
            }
            Peer peer;
            peer.id = gsl::narrow_cast<uint16_t>(sequencer_id);
            peer.host_addr = value["hostAddr"].get<std::string>();
            peer.raft_port = value["raftPort"].get<uint16_t>();
            peer.whisper_port = value["whisperPort"].get<uint16_t>();
            peer.engine_conn_port = value["engineConnPort"].get<uint16_t>();
            peers_[peer.id] = std::move(peer);
        }
    } catch (const json::exception& e) {
        LOG(ERROR) << "Invalid config file: " << e.what();
        return false;
    }
    return true;
}

bool SequencerConfig::LoadFromFile(std::string_view file_path) {
    std::string json_contents;
    if (!fs_utils::ReadContents(file_path, &json_contents)) {
        LOG(ERROR) << fmt::format("Failed to read file: {}", file_path);
        return false;
    }
    return Load(json_contents);
}

const SequencerConfig::Peer* SequencerConfig::GetPeer(uint16_t id) const {
    if (peers_.contains(id)) {
        return &peers_.at(id);
    } else {
        return nullptr;
    }
}

void SequencerConfig::ForEachPeer(std::function<void(const Peer*)> cb) const {
    for (const auto& entry : peers_) {
        cb(&entry.second);
    }
}

}  // namespace faas
