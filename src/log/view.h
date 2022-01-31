#pragma once

#include "base/common.h"
#include "utils/hash.h"
#include "utils/bits.h"

__BEGIN_THIRD_PARTY_HEADERS
#include "proto/shared_log.pb.h"
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace log {

// View and its inner class will never change after construction
class View {
public:
    explicit View(const ViewProto& view_proto);
    ~View() = default;

    uint16_t id() const { return id_; }

    size_t metalog_replicas() const { return metalog_replicas_; }
    size_t userlog_replicas() const { return userlog_replicas_; }
    size_t index_replicas() const { return index_replicas_; }
    size_t num_phylogs() const { return num_phylogs_; }

    size_t num_engine_nodes() const { return engine_node_ids_.size(); }
    size_t num_sequencer_nodes() const { return sequencer_node_ids_.size(); }
    size_t num_storage_nodes() const { return storage_node_ids_.size(); }

    using NodeIdVec = absl::FixedArray<uint16_t>;
    const NodeIdVec& GetEngineNodes() const { return engine_node_ids_; }
    const NodeIdVec& GetSequencerNodes() const { return sequencer_node_ids_; }
    const NodeIdVec& GetStorageNodes() const { return storage_node_ids_; }

    bool contains_engine_node(uint16_t node_id) const {
        return engine_nodes_.contains(node_id);
    }
    bool contains_sequencer_node(uint16_t node_id) const {
        return sequencer_nodes_.contains(node_id);
    }
    bool contains_storage_node(uint16_t node_id) const {
        return storage_nodes_.contains(node_id);
    }
    bool is_active_phylog(uint16_t sequencer_node_id) const {
        return active_phylogs_.contains(sequencer_node_id);
    }

    uint32_t LogSpaceIdentifier(uint32_t user_logspace) const {
        uint64_t h = hash::xxHash64(user_logspace, /* seed= */ log_space_hash_seed_);
        uint16_t node_id = log_space_hash_tokens_[h % log_space_hash_tokens_.size()];
        DCHECK(sequencer_nodes_.contains(node_id));
        return bits::JoinTwo16(id_, node_id);
    }

    uint64_t log_space_hash_seed() const { return log_space_hash_seed_; }
    const NodeIdVec& log_space_hash_tokens() const { return log_space_hash_tokens_; }

    class Engine {
    public:
        Engine(Engine&& other) = default;
        ~Engine() = default;

        const View* view() const { return view_; }
        uint16_t node_id() const { return node_id_; }

        const View::NodeIdVec& GetStorageNodes() const {
            return storage_nodes_;
        }

        uint16_t PickStorageNode() const {
            size_t idx = __atomic_fetch_add(&next_storage_node_, 1, __ATOMIC_RELAXED);
            return storage_nodes_.at(idx % storage_nodes_.size());
        }

        bool HasIndexFor(uint16_t sequencer_node_id) const {
            return indexed_sequencer_node_set_.contains(sequencer_node_id);
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        View::NodeIdVec storage_nodes_;
        absl::flat_hash_set<uint16_t> indexed_sequencer_node_set_;

        mutable size_t next_storage_node_;

        Engine(const View* view, uint16_t node_id,
               const View::NodeIdVec& storage_nodes,
               const View::NodeIdVec& index_sequencer_nodes);
        DISALLOW_IMPLICIT_CONSTRUCTORS(Engine);
    };

    const Engine* GetEngineNode(uint16_t node_id) const {
        DCHECK(engine_nodes_.contains(node_id));
        return engine_nodes_.at(node_id);
    }

    class Sequencer {
    public:
        Sequencer(Sequencer&& other) = default;
        ~Sequencer() = default;

        const View* view() const { return view_; }
        uint16_t node_id() const { return node_id_; }

        const View::NodeIdVec& GetReplicaSequencerNodes() const {
            return replica_sequencer_nodes_;
        }

        bool IsReplicaSequencerNode(uint16_t sequencer_node_id) const {
            return replica_sequencer_node_set_.contains(sequencer_node_id);
        }

        const View::NodeIdVec& GetIndexEngineNodes() const {
            return index_engine_nodes_;
        }

        bool IsIndexEngineNode(uint16_t engine_node_id) const {
            return index_engine_node_set_.contains(engine_node_id);
        }

        uint16_t PickIndexEngineNode() const {
            size_t idx = __atomic_fetch_add(&next_index_engine_node_, 1, __ATOMIC_RELAXED);
            return index_engine_nodes_.at(idx % index_engine_nodes_.size());
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        View::NodeIdVec replica_sequencer_nodes_;
        View::NodeIdVec index_engine_nodes_;
        absl::flat_hash_set<uint16_t> replica_sequencer_node_set_;
        absl::flat_hash_set<uint16_t> index_engine_node_set_;

        mutable size_t next_index_engine_node_;

        Sequencer(const View* view, uint16_t node_id,
                  const View::NodeIdVec& replica_sequencer_nodes,
                  const View::NodeIdVec& index_engine_nodes);
        DISALLOW_IMPLICIT_CONSTRUCTORS(Sequencer);
    };

    const Sequencer* GetSequencerNode(uint16_t node_id) const {
        DCHECK(sequencer_nodes_.contains(node_id));
        return sequencer_nodes_.at(node_id);
    }

    class Storage {
    public:
        Storage(Storage&& other) = default;
        ~Storage() = default;

        const View* view() const { return view_; }
        uint16_t node_id() const { return node_id_; }

        const View::NodeIdVec& GetSourceEngineNodes() const {
            return source_engine_nodes_;
        }

        bool IsSourceEngineNode(uint16_t engine_node_id) const {
            return source_engine_node_set_.contains(engine_node_id);
        }

    private:
        friend class View;
        const View* view_;
        uint16_t node_id_;

        View::NodeIdVec source_engine_nodes_;
        absl::flat_hash_set<uint16_t> source_engine_node_set_;

        Storage(const View* view, uint16_t node_id,
                const View::NodeIdVec& source_engine_nodes);
        DISALLOW_IMPLICIT_CONSTRUCTORS(Storage);
    };

    const Storage* GetStorageNode(uint16_t node_id) const {
        DCHECK(storage_nodes_.contains(node_id));
        return storage_nodes_.at(node_id);
    }

private:
    uint16_t id_;

    size_t metalog_replicas_;
    size_t userlog_replicas_;
    size_t index_replicas_;
    size_t num_phylogs_;

    NodeIdVec engine_node_ids_;
    NodeIdVec sequencer_node_ids_;
    NodeIdVec storage_node_ids_;

    absl::flat_hash_set<uint16_t> active_phylogs_;

    absl::InlinedVector<Engine, 16>    engines_;
    absl::InlinedVector<Sequencer, 16> sequencers_;
    absl::InlinedVector<Storage, 16>   storages_;

    absl::flat_hash_map<uint16_t, Engine*>    engine_nodes_;
    absl::flat_hash_map<uint16_t, Sequencer*> sequencer_nodes_;
    absl::flat_hash_map<uint16_t, Storage*>   storage_nodes_;

    uint64_t  log_space_hash_seed_;
    NodeIdVec log_space_hash_tokens_;

    DISALLOW_COPY_AND_ASSIGN(View);
};

}  // namespace log
}  // namespace faas
