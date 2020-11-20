#pragma once

#include "base/common.h"

namespace faas {
namespace sequencer {

typedef absl::flat_hash_map</* sequencer_id */ uint16_t,
                            /* address */ std::string> SequencerMap;
bool ParseSequencerMap(std::string_view file_path, SequencerMap* sequencer_map);

}  // namespace sequencer
}  // namespace faas
