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
        SetupJournalIndex(db_path);
    }
    SetupSeqnumDB(db_path);
}

StorageIndexer::~StorageIndexer() {
    if (journal_index_ != nullptr) {
        auto status = journal_index_->Close();
        TKRZW_CHECK_OK(status, Close);
    }
    if (seqnum_db_ != nullptr) {
        auto status = seqnum_db_->Close();
        TKRZW_CHECK_OK(status, Close);
    }
}

void StorageIndexer::SetupJournalIndex(std::string_view db_path) {
    tkrzw::TreeDBM::TuningParameters params;
    std::string path = fmt::format("{}/journal_index.tkt", db_path);
    tkrzw::TreeDBM* dbm = new tkrzw::TreeDBM();
    auto status = dbm->OpenAdvanced(path, /* writable= */ true,
                                    tkrzw::File::OPEN_DEFAULT, params);
    TKRZW_CHECK_OK(status, Open);
    journal_index_ = absl::WrapUnique(dbm);
}

void StorageIndexer::SetupSeqnumDB(std::string_view db_path) {
    tkrzw::TreeDBM::TuningParameters params;
    std::string path = fmt::format("{}/seqnum_db.tkt", db_path);
    tkrzw::TreeDBM* dbm = new tkrzw::TreeDBM();
    auto status = dbm->OpenAdvanced(path, /* writable= */ true,
                                    tkrzw::File::OPEN_DEFAULT, params);
    TKRZW_CHECK_OK(status, Open);
    seqnum_db_ = absl::WrapUnique(dbm);
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

static constexpr const char* kMarkStr = "MARK";

}  // namespace

void StorageIndexer::PutJournalIndex(const Record& record) {
    std::string key = bits::HexStr(record.seqnum);
    uint64_t encoded;
    EncodeJournalLocation(record.journal_file_id, record.journal_offset, &encoded);
    std::span<const char> value = VAR_AS_CHAR_SPAN(encoded, uint64_t);
    auto status = journal_index_->Set(
        key, std::string_view(value.data(), value.size()));
    TKRZW_CHECK_OK(status, Set);
}

void StorageIndexer::PutSeqnumDB(const Record& record) {
    std::string key = bits::HexStr(record.user_logspace) + bits::HexStr(record.seqnum);
    DCHECK_EQ(key.size(), 24U);
    auto status = seqnum_db_->Set(key, kMarkStr);
    TKRZW_CHECK_OK(status, Set);
}

void StorageIndexer::Put(const Record& record) {
    if (journal_index_ != nullptr) {
        PutJournalIndex(record);
    }
    PutSeqnumDB(record);
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

namespace {

class SeqnumRecordProcessor : public tkrzw::DBM::RecordProcessor {
public:
    SeqnumRecordProcessor(uint32_t user_logspace, uint64_t trim_seqnum)
        : key_prefix_(bits::HexStr(user_logspace)),
          trim_seqnum_(trim_seqnum),
          finished_(false) {}

    bool finished() const { return finished_; }

    void MarkFinished() {
        DCHECK(!finished_);
        finished_ = true;
    }

    void GrabSeqnums(StorageIndexer::SeqnumVec* seqnums) {
        DCHECK(finished_);
        *seqnums = std::move(seqnums_);
    }

    std::string_view ProcessFull(std::string_view key,
                                 std::string_view value) override {
        DCHECK(!finished_);
        if (!absl::StartsWith(key, key_prefix_)) {
            MarkFinished();
            return tkrzw::DBM::RecordProcessor::NOOP;
        }
        uint64_t seqnum = HexToInt(absl::StripPrefix(key, key_prefix_));
        if (seqnum >= trim_seqnum_) {
            MarkFinished();
            return tkrzw::DBM::RecordProcessor::NOOP;
        }
        seqnums_.push_back(seqnum);
        return tkrzw::DBM::RecordProcessor::REMOVE;
    }

private:
    std::string key_prefix_;
    uint64_t trim_seqnum_;
    bool finished_;

    StorageIndexer::SeqnumVec seqnums_;

    static uint64_t HexToInt(std::string_view str) {
        uint64_t value = 0;
        for (const char ch : str) {
            uint64_t t = 0;
            if ('0' <= ch && ch <= '9') {
                t = static_cast<uint64_t>(ch - '0');
            } else if ('a' <= ch && ch <= 'f') {
                t = static_cast<uint64_t>(ch - 'a') + 10;
            } else {
                LOG(FATAL) << "Invalid hex str: " << str;
            }
            value = (value << 4) + t;
        }
        return value;
    }

    DISALLOW_COPY_AND_ASSIGN(SeqnumRecordProcessor);
};

}  // namespace

void StorageIndexer::TrimSeqnumsUntil(uint32_t user_logspace,
                                      uint64_t trim_seqnum,
                                      SeqnumVec* trimmed_seqnums) {
    DCHECK(trimmed_seqnums != nullptr && trimmed_seqnums->empty());
    auto iter = seqnum_db_->MakeIterator();
    std::string starting_key = bits::HexStr(user_logspace) + std::string('0', 16);
    DCHECK_EQ(starting_key.size(), 24U);
    auto status = iter->JumpUpper(starting_key, /* inclusive= */ true);
    TKRZW_CHECK_OK(status, JumpUpper);
    SeqnumRecordProcessor proc(user_logspace, trim_seqnum);
    while (!proc.finished()) {
        auto status = iter->Process(&proc, /* writable= */ true);
        if (status == tkrzw::Status::NOT_FOUND_ERROR) {
            proc.MarkFinished();
            break;
        }
        TKRZW_CHECK_OK(status, Process);
    }
    proc.GrabSeqnums(trimmed_seqnums);
}

}  // namespace log
}  // namespace faas
