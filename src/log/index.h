#pragma once

#include "log/log_space_base.h"

namespace faas {
namespace log {

struct IndexQuery {
    enum ReadDirection { kReadNext, kReadPrev };
    ReadDirection direction;
    uint16_t origin_node_id;
    uint64_t client_data;

    uint32_t user_logspace;
    uint64_t user_tag;
    uint64_t query_seqnum;
    uint64_t metalog_progress;
};

struct IndexFoundResult {
    const View::Engine* engine_node;
    uint64_t seqnum;
    uint64_t metalog_progress;
};

struct IndexQueryResult {
    enum State { kFound, kEmpty, kContinue };
    State state;

    IndexQuery original_query;
    IndexFoundResult found_result;
};

class Index final : public LogSpaceBase {
public:
    Index(const View* view, uint16_t sequencer_id);
    ~Index();

    void ProvideIndexData(const IndexDataProto& index_data);

    void MakeQuery(const IndexQuery& query);

    typedef absl::InlinedVector<IndexQueryResult, 4> QueryResultVec;
    void PollQueryResults(QueryResultVec* results);

private:
    class PerSpaceIndex;
    absl::flat_hash_map</* user_logspace */ uint32_t,
                        std::unique_ptr<PerSpaceIndex>> index_;

    std::multimap</* metalog_position */ uint32_t,
                  IndexQuery> pending_queries_;
    QueryResultVec pending_query_results_;

    std::deque<std::pair</* metalog_seqnum */ uint32_t,
                        /* end_seqnum */ uint32_t>> cuts_;
    uint32_t indexed_metalog_position_;

    struct IndexData {
        uint16_t engine_id;
        uint32_t user_logspace;
        uint64_t user_tag;
    };
    std::map</* seqnum */ uint32_t, IndexData> received_data_;
    uint32_t data_received_seqnum_position_;
    uint32_t indexed_seqnum_position_;

    void OnMetaLogApplied(const MetaLogProto& meta_log_proto) override;
    void AdvanceIndexProgress();
    PerSpaceIndex* GetOrCreateIndex(uint32_t user_logspace);
    
    void ProcessQuery(const IndexQuery& query);
    IndexQueryResult BuildFoundResult(const IndexQuery& query,
                                      uint64_t seqnum, uint16_t engine_id);
    IndexQueryResult BuildNotFoundResult(const IndexQuery& query);

    DISALLOW_COPY_AND_ASSIGN(Index);
};

}  // namespace log
}  // namespace faas
