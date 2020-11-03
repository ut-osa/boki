#include "log/view.h"

namespace faas {
namespace log {

View::View(uint16_t id, const std::vector<uint16_t> node_ids)
    : id_(id),
      node_ids_(node_ids) {
    for (size_t i = 0; i < node_ids_.size(); i++) {
        node_indices_[node_ids_[i]] = i;
    }
}

View::~View() {
}

std::string_view View::get_host(uint16_t node_id) const {
    return "";
}

uint16_t View::get_port(uint16_t node_id) const {
    return 0;
}

uint16_t View::get_replica_node(uint16_t primary_node_id, size_t idx) const {
    DCHECK(node_indices_.contains(primary_node_id));
    size_t base = node_indices_.at(primary_node_id);
    return node_ids_[(base + idx) % node_ids_.size()];
}

std::unique_ptr<View> View::BuildFromMessage(std::span<const char> message) {
    return nullptr;
}

}  // namespace log
}  // namespace faas
