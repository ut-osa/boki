#include "log/db.h"

#include "utils/bits.h"
#include "utils/appendable_buffer.h"
#include "utils/fs.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <rocksdb/rate_limiter.h>
#include <tkrzw_dbm_hash.h>
#include <tkrzw_dbm_tree.h>
#include <tkrzw_dbm_skip.h>
#include <tkrzw_file_mmap.h>
#include <tkrzw_file_pos.h>
__END_THIRD_PARTY_HEADERS

// RocksDB tunables
ABSL_FLAG(int, rocksdb_max_background_jobs, 2, "");
ABSL_FLAG(bool, rocksdb_enable_compression, false, "");
ABSL_FLAG(size_t, rocksdb_write_buffer_size_mb, 64, "");
ABSL_FLAG(size_t, rocksdb_block_cache_size_mb, 32, "");
ABSL_FLAG(size_t, rocksdb_max_mbytes_for_level_base, 256, "");
ABSL_FLAG(size_t, rocksdb_mbytes_per_sync, 1, "");
ABSL_FLAG(size_t, rocksdb_max_total_wal_size_mb, 16, "");
ABSL_FLAG(size_t, rocksdb_rate_mbytes_per_sec, 0, "");
ABSL_FLAG(bool, rocksdb_use_direct_io, false, "");

// Tkrzw tunables
ABSL_FLAG(std::string, tkrzw_file_type, "mmap-para", "");

#define ROCKSDB_CHECK_OK(STATUS_VAR, OP_NAME)               \
    do {                                                    \
        if (!(STATUS_VAR).ok()) {                           \
            LOG(FATAL) << "RocksDB::" #OP_NAME " failed: "  \
                       << (STATUS_VAR).ToString();          \
        }                                                   \
    } while (0)

#define TKRZW_CHECK_OK(STATUS_VAR, OP_NAME)                 \
    do {                                                    \
        if (!(STATUS_VAR).IsOK()) {                         \
            LOG(FATAL) << "Tkrzw::" #OP_NAME " failed: "    \
                       << tkrzw::ToString(STATUS_VAR);      \
        }                                                   \
    } while (0)

#define LOG_HEADER "LogDB: "

static constexpr size_t kMBytesMultiplier = size_t{1}<<20;

namespace faas {
namespace log {

RocksDBBackend::RocksDBBackend(std::string_view db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.max_background_jobs = absl::GetFlag(
        FLAGS_rocksdb_max_background_jobs);
    options.bytes_per_sync = kMBytesMultiplier * absl::GetFlag(
        FLAGS_rocksdb_mbytes_per_sync);
    options.wal_bytes_per_sync = kMBytesMultiplier * absl::GetFlag(
        FLAGS_rocksdb_mbytes_per_sync);
    options.max_total_wal_size = kMBytesMultiplier* absl::GetFlag(
        FLAGS_rocksdb_max_total_wal_size_mb);
    options.use_direct_io_for_flush_and_compaction = absl::GetFlag(
        FLAGS_rocksdb_use_direct_io);
    size_t rate_mbytes_per_sec = absl::GetFlag(FLAGS_rocksdb_rate_mbytes_per_sec);
    if (rate_mbytes_per_sec > 0) {
        int64_t rate_limit = static_cast<int64_t>(kMBytesMultiplier * rate_mbytes_per_sec);
        options.rate_limiter.reset(rocksdb::NewGenericRateLimiter(rate_limit));
    }
    rocksdb::DB* db;
    HLOG_F(INFO, "Open RocksDB at path {}", db_path);
    auto status = rocksdb::DB::Open(options, std::string(db_path), &db);
    ROCKSDB_CHECK_OK(status, Open);
    db_ = absl::WrapUnique(db);
}

RocksDBBackend::~RocksDBBackend() {}

void RocksDBBackend::InstallLogSpace(uint32_t logspace_id) {
    HLOG_F(INFO, "Install log space {}", bits::HexStr0x(logspace_id));
    rocksdb::ColumnFamilyOptions options;
    options.write_buffer_size = kMBytesMultiplier * absl::GetFlag(
        FLAGS_rocksdb_write_buffer_size_mb);
    options.max_bytes_for_level_base = kMBytesMultiplier * absl::GetFlag(
        FLAGS_rocksdb_max_mbytes_for_level_base);
    if (absl::GetFlag(FLAGS_rocksdb_enable_compression)) {
        options.compression = rocksdb::kZSTD;
    } else {
        options.compression = rocksdb::kNoCompression;
    }
    options.OptimizeForPointLookup(
        absl::GetFlag(FLAGS_rocksdb_block_cache_size_mb));
    rocksdb::ColumnFamilyHandle* cf_handle = nullptr;
    auto status = db_->CreateColumnFamily(
        options, bits::HexStr(logspace_id), &cf_handle);
    ROCKSDB_CHECK_OK(status, CreateColumnFamily);
    {
        absl::MutexLock lk(&mu_);
        DCHECK(!column_families_.contains(logspace_id));
        column_families_[logspace_id].reset(DCHECK_NOTNULL(cf_handle));
    }
}

std::optional<std::string> RocksDBBackend::Get(uint32_t logspace_id, uint32_t key) {
    rocksdb::ColumnFamilyHandle* cf_handle = GetCFHandle(logspace_id);
    if (cf_handle == nullptr) {
        HLOG_F(WARNING, "Log space {} not created", bits::HexStr0x(logspace_id));
        return std::nullopt;
    }
    std::string key_str = bits::HexStr(key);
    std::string data;
    auto status = db_->Get(rocksdb::ReadOptions(), cf_handle, key_str, &data);
    if (status.IsNotFound()) {
        return std::nullopt;
    }
    ROCKSDB_CHECK_OK(status, Get);
    return data;
}

void RocksDBBackend::PutBatch(const Batch& batch) {
    DCHECK_EQ(batch.keys.size(), batch.data.size());
    rocksdb::ColumnFamilyHandle* cf_handle = GetCFHandle(batch.logspace_id);
    if (cf_handle == nullptr) {
        HLOG_F(ERROR, "Log space {} not created", bits::HexStr0x(batch.logspace_id));
        return;
    }
    rocksdb::WriteBatch write_batch;
    for (size_t i = 0; i < batch.keys.size(); i++) {
        std::string key_str = bits::HexStr(batch.keys[i]);
        write_batch.Put(cf_handle, key_str, batch.data[i]);
    }
    auto status = db_->Write(rocksdb::WriteOptions(), &write_batch);
    ROCKSDB_CHECK_OK(status, Write);
    status = db_->Flush(rocksdb::FlushOptions(), cf_handle);
    ROCKSDB_CHECK_OK(status, Flush);
}

void RocksDBBackend::Delete(uint32_t logspace_id, std::span<const uint32_t> keys) {
    rocksdb::ColumnFamilyHandle* cf_handle = GetCFHandle(logspace_id);
    if (cf_handle == nullptr) {
        HLOG_F(ERROR, "Log space {} not created", bits::HexStr0x(logspace_id));
        return;
    }
    rocksdb::WriteBatch write_batch;
    for (size_t i = 0; i < keys.size(); i++) {
        std::string key_str = bits::HexStr(keys[i]);
        write_batch.Delete(cf_handle, key_str);
    }
    auto status = db_->Write(rocksdb::WriteOptions(), &write_batch);
    ROCKSDB_CHECK_OK(status, Write);
    status = db_->Flush(rocksdb::FlushOptions(), cf_handle);
    ROCKSDB_CHECK_OK(status, Flush);
}

void RocksDBBackend::StagingPut(std::string_view key, std::span<const char> data) {
    auto status = db_->Put(rocksdb::WriteOptions(), key,
                           rocksdb::Slice(data.data(), data.size()));
    ROCKSDB_CHECK_OK(status, Put);
}

void RocksDBBackend::StagingDelete(std::string_view key) {
    auto status = db_->Delete(rocksdb::WriteOptions(), key);
    ROCKSDB_CHECK_OK(status, Delete);
}

void RocksDBBackend::GetStat(size_t* num_keys, size_t* byte_size) {
    uint64_t value;
    CHECK(db_->GetAggregatedIntProperty(rocksdb::DB::Properties::kEstimateNumKeys, &value));
    *num_keys = value;
    CHECK(db_->GetAggregatedIntProperty(rocksdb::DB::Properties::kEstimateLiveDataSize, &value));
    *byte_size = value;
}

rocksdb::ColumnFamilyHandle* RocksDBBackend::GetCFHandle(uint32_t logspace_id) {
    absl::ReaderMutexLock lk(&mu_);
    if (!column_families_.contains(logspace_id)) {
        return nullptr;
    }
    return column_families_.at(logspace_id).get();
}

TkrzwDBMBackend::TkrzwDBMBackend(Type type, std::string_view db_path)
    : type_(type),
      db_path_(db_path) {
    staging_db_ = CreateDBM("staging");
}

TkrzwDBMBackend::~TkrzwDBMBackend() {
    for (const auto& [logspace_id, dbm] : dbs_) {
        auto status = dbm->Close();
        TKRZW_CHECK_OK(status, Close);
    }
    auto status = staging_db_->Close();
    TKRZW_CHECK_OK(status, Close);
}

void TkrzwDBMBackend::InstallLogSpace(uint32_t logspace_id) {
    HLOG_F(INFO, "Install log space {}", bits::HexStr0x(logspace_id));
    auto db = CreateDBM(bits::HexStr(logspace_id));
    {
        absl::MutexLock lk(&mu_);
        DCHECK(!dbs_.contains(logspace_id));
        dbs_[logspace_id] = std::move(db);
    }
}

std::optional<std::string> TkrzwDBMBackend::Get(uint32_t logspace_id, uint32_t key) {
    tkrzw::DBM* dbm = GetDBM(logspace_id);
    if (dbm == nullptr) {
        HLOG_F(WARNING, "Log space {} not created", bits::HexStr0x(logspace_id));
        return std::nullopt;
    }
    std::string key_str = bits::HexStr(key);
    std::string data;
    auto status = dbm->Get(key_str, &data);
    if (status == tkrzw::Status::NOT_FOUND_ERROR) {
        return std::nullopt;
    }
    TKRZW_CHECK_OK(status, Get);
    return data;
}

void TkrzwDBMBackend::PutBatch(const Batch& batch) {
    DCHECK_EQ(batch.keys.size(), batch.data.size());
    tkrzw::DBM* dbm = GetDBM(batch.logspace_id);
    if (dbm == nullptr) {
        HLOG_F(ERROR, "Log space {} not created", bits::HexStr0x(batch.logspace_id));
        return;
    }
    for (size_t i = 0; i < batch.keys.size(); i++) {
        auto status = dbm->Set(bits::HexStr(batch.keys[i]), batch.data[i]);
        TKRZW_CHECK_OK(status, Set);
    }
    auto status = dbm->Synchronize(/* hard= */ false);
    TKRZW_CHECK_OK(status, Synchronize);
}

void TkrzwDBMBackend::Delete(uint32_t logspace_id, std::span<const uint32_t> keys) {
    tkrzw::DBM* dbm = GetDBM(logspace_id);
    if (dbm == nullptr) {
        HLOG_F(ERROR, "Log space {} not created", bits::HexStr0x(logspace_id));
        return;
    }
    for (uint32_t key : keys) {
        auto status = dbm->Remove(bits::HexStr(key));
        if (status == tkrzw::Status::NOT_FOUND_ERROR) {
            HLOG_F(WARNING, "Failed to find seqnum {} in DB",
                   bits::HexStr0x(bits::JoinTwo32(logspace_id, key)));
            continue;
        }
        TKRZW_CHECK_OK(status, Remove);
    }
    auto status = dbm->Synchronize(/* hard= */ false);
    TKRZW_CHECK_OK(status, Synchronize);
}

void TkrzwDBMBackend::StagingPut(std::string_view key, std::span<const char> data) {
    auto status = staging_db_->Set(key, std::string_view(data.data(), data.size()));
    TKRZW_CHECK_OK(status, Set);
}

void TkrzwDBMBackend::StagingDelete(std::string_view key) {
    auto status = staging_db_->Remove(key);
    TKRZW_CHECK_OK(status, Set);
}

void TkrzwDBMBackend::GetStat(size_t* num_keys, size_t* byte_size) {
    *num_keys = 0;
    *byte_size = 0;
    tkrzw::Status status;
    absl::ReaderMutexLock lk(&mu_);
    for (const auto& [_, dbm]: dbs_) {
        int64_t value;
        status = dbm->Count(&value);
        TKRZW_CHECK_OK(status, Count);
        DCHECK_GE(value, 0);
        *num_keys += static_cast<size_t>(value);
        status = dbm->GetFileSize(&value);
        TKRZW_CHECK_OK(status, GetFileSize);
        DCHECK_GE(value, 0);
        *byte_size += static_cast<size_t>(value);
    }
}

std::unique_ptr<tkrzw::DBM> TkrzwDBMBackend::CreateDBM(std::string_view name) {
    std::unique_ptr<tkrzw::File> file;
    std::string file_type = absl::GetFlag(FLAGS_tkrzw_file_type);
    if (file_type == "mmap-para") {
        file = std::make_unique<tkrzw::MemoryMapParallelFile>();
    } else if (file_type == "pos-para") {
        file = std::make_unique<tkrzw::PositionalParallelFile>();
    } else {
        LOG(FATAL) << "Unknown Tkrzw file type: " << file_type;
    }
    tkrzw::DBM* db_ptr = nullptr;
    if (type_ == kHashDBM) {
        tkrzw::HashDBM* db = new tkrzw::HashDBM(std::move(file));
        auto status = db->OpenAdvanced(
            /* path= */ fs_utils::JoinPath(db_path_, fmt::format("{}.tkh", name)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ tkrzw::HashDBM::TuningParameters());
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else if (type_ == kTreeDBM) {
        tkrzw::TreeDBM* db = new tkrzw::TreeDBM(std::move(file));
        auto status = db->OpenAdvanced(
            /* path= */ fs_utils::JoinPath(db_path_, fmt::format("{}.tkt", name)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ tkrzw::TreeDBM::TuningParameters());
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else if (type_ == kSkipDBM) {
        tkrzw::SkipDBM* db = new tkrzw::SkipDBM(std::move(file));
        auto status = db->OpenAdvanced(
            /* path= */ fs_utils::JoinPath(db_path_, fmt::format("{}.tks", name)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ tkrzw::SkipDBM::TuningParameters());
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else {
        UNREACHABLE();
    }
    return absl::WrapUnique(db_ptr);
}

tkrzw::DBM* TkrzwDBMBackend::GetDBM(uint32_t logspace_id) {
    absl::ReaderMutexLock lk(&mu_);
    if (!dbs_.contains(logspace_id)) {
        return nullptr;
    }
    return dbs_.at(logspace_id).get();
}

}  // namespace log
}  // namespace faas
