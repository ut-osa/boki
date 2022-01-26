#include "log/storage_indexer.h"

#include "utils/bits.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_tree.h>
__END_THIRD_PARTY_HEADERS

#define TKRZW_CHECK_OK(STATUS_VAR, OP_NAME)                 \
    do {                                                    \
        if (!(STATUS_VAR).IsOK()) {                         \
            LOG(FATAL) << "Tkrzw::" #OP_NAME " failed: "    \
                       << tkrzw::ToString(STATUS_VAR);      \
        }                                                   \
    } while (0)

namespace faas {
namespace log {

StorageIndexer::StorageIndexer(std::string_view db_path, bool journal_enabled) {
    if (journal_enabled) {
        tkrzw::TreeDBM::TuningParameters params;
        std::string path = fmt::format("{}/journal_index.tkt", db_path);
        tkrzw::TreeDBM* dbm = new tkrzw::TreeDBM();
        auto status = dbm->OpenAdvanced(path, /* writable= */ true,
                                        tkrzw::File::OPEN_DEFAULT, params);
        TKRZW_CHECK_OK(status, Open);
        journal_index_ = absl::WrapUnique(dbm);
    }
}

StorageIndexer::~StorageIndexer() {
    if (journal_index_ != nullptr) {
        auto status = journal_index_->Close();
        TKRZW_CHECK_OK(status, Close);
    }
}

namespace {

void EncodeJournalLocation(int file_id, size_t offset, uint64_t* encoded) {
    DCHECK_GE(file_id, 0);
    DCHECK_LT(file_id, 1<<16);
    DCHECK_EQ(offset >> 48, 0U);
    *encoded = static_cast<uint64_t>(file_id) << 48;
    *encoded += static_cast<uint64_t>(offset);
}

void DecodeJournalLocation(uint64_t encoded, int* file_id, size_t* offset) {
    *file_id = gsl::narrow_cast<int>(encoded >> 48);
    constexpr uint64_t kMask = (1ULL<<48)-1;
    *offset = static_cast<size_t>(encoded & kMask);
}

}  // namespace

void StorageIndexer::Put(const Record& record) {
    if (journal_index_ != nullptr) {
        std::string key = bits::HexStr(record.seqnum);
        uint64_t encoded;
        EncodeJournalLocation(record.journal_file_id, record.journal_offset, &encoded);
        std::span<const char> value = VAR_AS_CHAR_SPAN(encoded, uint64_t);
        auto status = journal_index_->Set(
            key, std::string_view(value.data(), value.size()));
        TKRZW_CHECK_OK(status, Set);
    }
}

bool StorageIndexer::GetJournalLocation(uint64_t seqnum,
                                        int* file_id, size_t* offset) {
    DCHECK_NOTNULL(journal_index_);
    std::string key = bits::HexStr(seqnum);
    std::string value;
    auto status = journal_index_->Get(key, &value);
    if (status == tkrzw::Status::NOT_FOUND_ERROR) {
        return false;
    }
    TKRZW_CHECK_OK(status, Get);
    DCHECK_EQ(value.size(), sizeof(uint64_t));
    const uint64_t* encoded = reinterpret_cast<const uint64_t*>(value.data());
    DecodeJournalLocation(*encoded, file_id, offset);
    return true;
}

}  // namespace log
}  // namespace faas

