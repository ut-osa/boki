#pragma once

#include "base/common.h"

namespace faas {

class SequencerConfig {
public:
    SequencerConfig();
    ~SequencerConfig();

    bool Load(std::string_view json_contents);
    bool LoadFromFile(std::string_view file_path);

    struct Peer {
        uint16_t    id;
        std::string host_addr;
        uint16_t    raft_port;
        uint16_t    whisper_port;
        uint16_t    engine_conn_port;
    };

    const Peer* GetPeer(uint16_t id) const;
    void ForEachPeer(std::function<void(const Peer*)> cb) const;

private:
    absl::flat_hash_map</* id */ uint16_t, Peer> peers_;

    DISALLOW_COPY_AND_ASSIGN(SequencerConfig);
};

}  // namespace faas
