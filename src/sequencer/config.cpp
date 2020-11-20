#include "sequencer/config.h"

#include "utils/fs.h"

#include <nlohmann/json.hpp>
using json = nlohmann::json;

namespace faas {
namespace sequencer {

bool ParseSequencerMap(std::string_view file_path, SequencerMap* sequencer_map) {
    std::string json_contents;
    if (!fs_utils::ReadContents(file_path, &json_contents)) {
        LOG(ERROR) << fmt::format("Failed to read file: {}", file_path);
        return false;
    }
    json config;
    try {
        config = json::parse(json_contents);
    } catch (const json::parse_error& e) {
        LOG(ERROR) << "Failed to parse json: " << e.what();
        return false;
    }
    sequencer_map->clear();
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
            std::string address = entry.value().get<std::string>();
            sequencer_map->insert(std::make_pair(
                gsl::narrow_cast<uint16_t>(sequencer_id), address));
        }
    } catch (const json::exception& e) {
        LOG(ERROR) << "Invalid config file: " << e.what();
        return false;
    }
    return true;
}

}  // namespace sequencer
}  // namespace faas
