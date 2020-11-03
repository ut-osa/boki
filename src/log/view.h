#pragma once

#include "log/common.h"

namespace faas {
namespace log {

class View {
public:
    View(uint16_t id, const std::vector<uint16_t> node_ids);
    ~View();

    uint16_t id() const { return id_; }
    bool has_node(uint16_t node_id) const { return node_indices_.contains(node_id); }
    std::string_view get_host(uint16_t node_id) const;
    uint16_t get_port(uint16_t node_id) const;
    uint16_t get_replica_node(uint16_t primary_node_id, size_t idx) const;

    static std::unique_ptr<View> BuildFromMessage(std::span<const char> message);

private:
    uint16_t id_;
    std::vector<uint16_t> node_ids_;
    absl::flat_hash_map</* node_id */ uint16_t, size_t> node_indices_;

    DISALLOW_COPY_AND_ASSIGN(View);
};

}  // namespace log
}  // namespace faas
