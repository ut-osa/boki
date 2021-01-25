#include "log/view.h"

namespace faas {
namespace log {

View::View(const ViewProto& view_proto)
    : id_(gsl::narrow_cast<uint16_t>(view_proto.view_id())),
      metalog_replicas_(view_proto.metalog_replicas()),
      userlog_replicas_(view_proto.userlog_replicas()),
      index_replicas_(view_proto.index_replicas()),
      engine_node_ids_(static_cast<size_t>(view_proto.engine_nodes_size())),
      sequencer_node_ids_(static_cast<size_t>(view_proto.sequencer_nodes_size())),
      storage_node_ids_(static_cast<size_t>(view_proto.storage_nodes_size())),
      log_space_hash_seed_(view_proto.log_space_hash_seed()),
      log_space_hash_tokens_(static_cast<size_t>(view_proto.log_space_hash_tokens_size())) {

    for (size_t i = 0; i < engine_node_ids_.size(); i++) {
        engine_node_ids_[i] = gsl::narrow_cast<uint16_t>(
            view_proto.engine_nodes(static_cast<int>(i)));
    }
    for (size_t i = 0; i < sequencer_node_ids_.size(); i++) {
        sequencer_node_ids_[i] = gsl::narrow_cast<uint16_t>(
            view_proto.sequencer_nodes(static_cast<int>(i)));
    }
    for (size_t i = 0; i < storage_node_ids_.size(); i++) {
        storage_node_ids_[i] = gsl::narrow_cast<uint16_t>(
            view_proto.storage_nodes(static_cast<int>(i)));
    }

    size_t num_engine_nodes = engine_node_ids_.size();
    size_t num_sequencer_nodes = sequencer_node_ids_.size();
    size_t num_storage_nodes = storage_node_ids_.size();

    absl::flat_hash_set<uint16_t> engine_node_id_set(
        engine_node_ids_.begin(), engine_node_ids_.end());
    DCHECK_EQ(engine_node_id_set.size(), num_engine_nodes);

    absl::flat_hash_set<uint16_t> sequencer_node_id_set(
        sequencer_node_ids_.begin(), sequencer_node_ids_.end());
    DCHECK_EQ(sequencer_node_id_set.size(), num_sequencer_nodes);

    absl::flat_hash_set<uint16_t> storage_node_id_set(
        storage_node_ids_.begin(), storage_node_ids_.end());
    DCHECK_EQ(storage_node_id_set.size(), num_storage_nodes);

    DCHECK_EQ(static_cast<size_t>(view_proto.index_plan_size()),
              num_sequencer_nodes * index_replicas_);
    absl::flat_hash_map<uint16_t, std::vector<uint16_t>> index_engine_nodes;
    absl::flat_hash_map<uint16_t, std::vector<uint16_t>> index_sequencer_nodes;
    for (size_t i = 0; i < num_sequencer_nodes; i++) {
        uint16_t sequencer_node_id = sequencer_node_ids_[i];
        for (size_t j = 0; j < index_replicas_; j++) {
            uint16_t engine_node_id = gsl::narrow_cast<uint16_t>(
                view_proto.index_plan(static_cast<int>(i * index_replicas_ + j)));
            DCHECK(engine_node_id_set.contains(engine_node_id));
            index_engine_nodes[sequencer_node_id].push_back(engine_node_id);
            index_sequencer_nodes[engine_node_id].push_back(sequencer_node_id);
        }
    }

    DCHECK_EQ(static_cast<size_t>(view_proto.storage_plan_size()),
              num_engine_nodes * userlog_replicas_);
    absl::flat_hash_map<uint16_t, std::vector<uint16_t>> storage_nodes;
    absl::flat_hash_map<uint16_t, std::vector<uint16_t>> source_engine_nodes;
    for (size_t i = 0; i < num_engine_nodes; i++) {
        uint16_t engine_node_id = engine_node_ids_[i];
        for (size_t j = 0; j < userlog_replicas_; j++) {
            uint16_t storage_node_id = gsl::narrow_cast<uint16_t>(
                view_proto.storage_plan(static_cast<int>(i * userlog_replicas_ + j)));
            DCHECK(storage_node_id_set.contains(storage_node_id));
            storage_nodes[engine_node_id].push_back(storage_node_id);
            source_engine_nodes[storage_node_id].push_back(engine_node_id);
        }
    }

    for (size_t i = 0; i < num_engine_nodes; i++) {
        uint16_t node_id = engine_node_ids_[i];
        engines_.push_back(Engine(
            this, node_id,
            NodeIdVec(storage_nodes[node_id].begin(),
                      storage_nodes[node_id].end()),
            NodeIdVec(index_sequencer_nodes[node_id].begin(),
                      index_sequencer_nodes[node_id].end())));
    }
    for (size_t i = 0; i < num_engine_nodes; i++) {
        engine_nodes_[engine_node_ids_[i]] = &engines_[i];
    }

    for (size_t i = 0; i < num_sequencer_nodes; i++) {
        std::vector<uint16_t> replica_sequencer_nodes;
        for (size_t j = 1; j < metalog_replicas_; j++) {
            replica_sequencer_nodes.push_back(
                sequencer_node_ids_[(i + j) % num_sequencer_nodes]);
        }
        uint16_t node_id = sequencer_node_ids_[i];
        sequencers_.push_back(Sequencer(
            this, node_id,
            NodeIdVec(replica_sequencer_nodes.begin(),
                      replica_sequencer_nodes.end()),
            NodeIdVec(index_engine_nodes[node_id].begin(),
                      index_engine_nodes[node_id].end())));
    }
    for (size_t i = 0; i < num_sequencer_nodes; i++) {
        sequencer_nodes_[sequencer_node_ids_[i]] = &sequencers_[i];
    }

    for (size_t i = 0; i < num_storage_nodes; i++) {
        uint16_t node_id = storage_node_ids_[i];
        storages_.push_back(Storage(
            this, node_id,
            NodeIdVec(source_engine_nodes[node_id].begin(),
                      source_engine_nodes[node_id].end())));
    }
    for (size_t i = 0; i < num_storage_nodes; i++) {
        storage_nodes_[storage_node_ids_[i]] = &storages_[i];
    }

    for (size_t i = 0; i < log_space_hash_tokens_.size(); i++) {
        uint16_t node_id = gsl::narrow_cast<uint16_t>(
            view_proto.log_space_hash_tokens(static_cast<int>(i)));
        DCHECK(sequencer_node_id_set.contains(node_id));
        log_space_hash_tokens_[i] = node_id;
    }
}

View::Engine::Engine(const View* view, uint16_t node_id,
                     const View::NodeIdVec& storage_nodes,
                     const View::NodeIdVec& index_sequencer_nodes)
    : view_(view),
      node_id_(node_id),
      storage_nodes_(storage_nodes),
      indexed_sequencer_node_set_(index_sequencer_nodes.begin(),
                                  index_sequencer_nodes.end()),
      next_storage_node_(0) {}

View::Sequencer::Sequencer(const View* view, uint16_t node_id,
                           const View::NodeIdVec& replica_sequencer_nodes,
                           const View::NodeIdVec& index_engine_nodes)
    : view_(view),
      node_id_(node_id),
      replica_sequencer_nodes_(replica_sequencer_nodes),
      index_engine_nodes_(index_engine_nodes),
      replica_sequencer_node_set_(replica_sequencer_nodes.begin(),
                                  replica_sequencer_nodes.end()),
      index_engine_node_set_(index_engine_nodes.begin(),
                             index_engine_nodes.end()),
      next_index_engine_node_(0) {}

View::Storage::Storage(const View* view, uint16_t node_id,
                       const View::NodeIdVec& source_engine_nodes)
    : view_(view),
      node_id_(node_id),
      source_engine_nodes_(source_engine_nodes),
      source_engine_node_set_(source_engine_nodes.begin(),
                              source_engine_nodes.end()) {}

}  // namespace log
}  // namespace faas
