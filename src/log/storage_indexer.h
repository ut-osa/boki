#pragma once

#include "log/common.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm.h>
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace log {

class StorageIndexer {
public:
    StorageIndexer(std::string_view db_path, bool journal_enabled);
    ~StorageIndexer();

    struct Record {
        uint64_t seqnum          {kInvalidLogSeqNum};
        uint32_t user_logspace   {0};
        int      journal_file_id {0};
        size_t   journal_offset  {0};
    };

    void Put(const Record& record);

    bool GetJournalLocation(uint64_t seqnum, int* file_id, size_t* offset);

    using SeqnumVec = absl::InlinedVector<uint64_t, 16>;
    void TrimSeqnumsUntil(uint32_t user_logspace, uint64_t trim_seqnum,
                          SeqnumVec* trimmed_seqnums);

private:
    std::unique_ptr<tkrzw::DBM> journal_index_;
    std::unique_ptr<tkrzw::DBM> seqnum_db_;

    void SetupJournalIndex(std::string_view db_path);
    void SetupSeqnumDB(std::string_view db_path);

    void PutJournalIndex(const Record& record);
    void PutSeqnumDB(const Record& record);

    DISALLOW_COPY_AND_ASSIGN(StorageIndexer);
};

}  // namespace log
}  // namespace faas
