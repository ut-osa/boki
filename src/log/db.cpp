#include "log/db.h"

#include "utils/bits.h"
#include "utils/appendable_buffer.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <rocksdb/rate_limiter.h>
#include <tkrzw_dbm_hash.h>
#include <tkrzw_dbm_tree.h>
#include <tkrzw_dbm_skip.h>
__END_THIRD_PARTY_HEADERS

// RocksDB tunables
ABSL_FLAG(int, rocksdb_max_background_jobs, 2, "");
ABSL_FLAG(bool, rocksdb_enable_compression, false, "");
ABSL_FLAG(size_t, rocksdb_write_buffer_size_mb, 64, "");
ABSL_FLAG(size_t, rocksdb_max_mbytes_for_level_base, 256, "");
ABSL_FLAG(size_t, rocksdb_mbytes_per_sync, 1, "");
ABSL_FLAG(size_t, rocksdb_max_total_wal_size_mb, 16, "");
ABSL_FLAG(size_t, rocksdb_rate_mbytes_per_sec, 128, "");
ABSL_FLAG(bool, rocksdb_use_direct_io, false, "");

// LMDB tunables
ABSL_FLAG(int, lmdb_maxdbs, 16, "");
ABSL_FLAG(size_t, lmdb_mapsize_mb, 1024, "");

#define ROCKSDB_CHECK_OK(STATUS_VAR, OP_NAME)               \
    do {                                                    \
        if (!(STATUS_VAR).ok()) {                           \
            LOG(FATAL) << "RocksDB::" #OP_NAME " failed: "  \
                       << (STATUS_VAR).ToString();          \
        }                                                   \
    } while (0)

#define LMDB_CHECK_OK(RETCODE_VAR, OP_NAME)                 \
    do {                                                    \
        if ((RETCODE_VAR) != 0) {                           \
            LOG(FATAL) << "LMDB::" #OP_NAME " failed: "     \
                       << mdb_strerror(RETCODE_VAR);        \
        }                                                   \
    } while (0)

#define TKRZW_CHECK_OK(STATUS_VAR, OP_NAME)                 \
    do {                                                    \
        if (!(STATUS_VAR).IsOK()) {                         \
            LOG(FATAL) << "Tkrzw::" #OP_NAME " failed: "    \
                       << tkrzw::ToString(STATUS_VAR);      \
        }                                                   \
    } while (0)

#define log_header_ "LogDB: "

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
    options.rate_limiter.reset(rocksdb::NewGenericRateLimiter(static_cast<int64_t>(
        kMBytesMultiplier * absl::GetFlag(FLAGS_rocksdb_rate_mbytes_per_sec))));
    rocksdb::DB* db;
    HLOG_F(INFO, "Open RocksDB at path {}", db_path);
    auto status = rocksdb::DB::Open(options, std::string(db_path), &db);
    ROCKSDB_CHECK_OK(status, Open);
    db_.reset(db);
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
    // Essentially disable block cache with 32MB in size
    options.OptimizeForPointLookup(/* block_cache_size_mb= */ 32);
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
}

rocksdb::ColumnFamilyHandle* RocksDBBackend::GetCFHandle(uint32_t logspace_id) {
    absl::ReaderMutexLock lk(&mu_);
    if (!column_families_.contains(logspace_id)) {
        return nullptr;
    }
    return column_families_.at(logspace_id).get();
}

LMDBBackend::LMDBBackend(std::string_view db_path) {
    int ret = 0;
    ret = mdb_env_create(&env_);
    LMDB_CHECK_OK(ret, EnvCreate);
    size_t mapsize = kMBytesMultiplier * absl::GetFlag(FLAGS_lmdb_mapsize_mb);
    ret = mdb_env_set_mapsize(env_, mapsize);
    LMDB_CHECK_OK(ret, EnvSetMapSize);
    MDB_dbi maxdbs = static_cast<MDB_dbi>(absl::GetFlag(FLAGS_lmdb_maxdbs));
    ret = mdb_env_set_maxdbs(env_, maxdbs);
    LMDB_CHECK_OK(ret, EnvSetMaxDbs);
    ret = mdb_env_open(
        /* env= */ env_,
        /* path= */ std::string(db_path).c_str(),
        /* flags= */ 0,
        /* mode= */ __FAAS_FILE_CREAT_MODE);
    LMDB_CHECK_OK(ret, EnvOpen);
}

LMDBBackend::~LMDBBackend() {
    mdb_env_close(env_);
}

void LMDBBackend::InstallLogSpace(uint32_t logspace_id) {
    int ret = 0;
    MDB_txn* txn;
    ret = mdb_txn_begin(env_, /* parent= */ nullptr, /* flags= */ 0, &txn);
    LMDB_CHECK_OK(ret, TxnBegin);
    std::string name(bits::HexStr(logspace_id));
    MDB_dbi dbi;
    ret = mdb_dbi_open(txn, name.c_str(), /* flags= */ MDB_INTEGERKEY | MDB_CREATE, &dbi);
    LMDB_CHECK_OK(ret, DdiOpen);
    ret = mdb_txn_commit(txn);
    LMDB_CHECK_OK(ret, TxnCommit);
    {
        absl::MutexLock lk(&mu_);
        DCHECK(!dbs_.contains(logspace_id));
        dbs_[logspace_id] = dbi;
    }
}

std::optional<std::string> LMDBBackend::Get(uint32_t logspace_id, uint32_t key) {
    MDB_dbi dbi;
    if (auto tmp = GetDB(logspace_id); tmp.has_value()) {
        dbi = tmp.value();
    } else {
        HLOG_F(WARNING, "Log space {} not created", bits::HexStr0x(logspace_id));
        return std::nullopt;
    }
    int ret = 0;
    MDB_txn* txn;
    ret = mdb_txn_begin(env_, /* parent= */ nullptr, /* flags= */ MDB_RDONLY, &txn);
    LMDB_CHECK_OK(ret, TxnBegin);
    MDB_val mdb_key {
        .mv_size = sizeof(uint32_t),
        .mv_data = &key
    };
    std::optional<std::string> result = std::nullopt;
    MDB_val data;
    ret = mdb_get(txn, dbi, &mdb_key, &data);
    if (ret == 0) {
        result.emplace(reinterpret_cast<const char*>(data.mv_data), data.mv_size);
    } else if (ret != MDB_NOTFOUND) {
        LMDB_CHECK_OK(ret, Get);
    }
    mdb_txn_abort(txn);
    return result;
}

void LMDBBackend::PutBatch(const Batch& batch) {
    DCHECK_EQ(batch.keys.size(), batch.data.size());
    MDB_dbi dbi;
    if (auto tmp = GetDB(batch.logspace_id); tmp.has_value()) {
        dbi = tmp.value();
    } else {
        HLOG_F(WARNING, "Log space {} not created", bits::HexStr0x(batch.logspace_id));
        return;
    }
    int ret = 0;
    MDB_txn* txn;
    ret = mdb_txn_begin(env_, /* parent= */ nullptr, /* flags= */ 0, &txn);
    LMDB_CHECK_OK(ret, TxnBegin);
    for (size_t i = 0; i < batch.keys.size(); i++) {
        MDB_val mdb_key {
            .mv_size = sizeof(uint32_t),
            .mv_data = const_cast<uint32_t*>(&batch.keys[i])
        };
        std::span<const char> data = batch.data[i];
        MDB_val mdb_data {
            .mv_size = data.size(),
            .mv_data = const_cast<char*>(data.data())
        };
        ret = mdb_put(txn, dbi, &mdb_key, &mdb_data, /* flags= */ 0);
        LMDB_CHECK_OK(ret, Put);
    }
    ret = mdb_txn_commit(txn);
    LMDB_CHECK_OK(ret, TxnCommit);
}

void LMDBBackend::Delete(uint32_t logspace_id, std::span<const uint32_t> keys) {
    NOT_IMPLEMENTED();
     MDB_dbi dbi;
    if (auto tmp = GetDB(logspace_id); tmp.has_value()) {
        dbi = tmp.value();
    } else {
        HLOG_F(WARNING, "Log space {} not created", bits::HexStr0x(logspace_id));
        return;
    }
    int ret = 0;
    MDB_txn* txn;
    ret = mdb_txn_begin(env_, /* parent= */ nullptr, /* flags= */ 0, &txn);
    LMDB_CHECK_OK(ret, TxnBegin);
    for (size_t i = 0; i < keys.size(); i++) {
        MDB_val mdb_key {
            .mv_size = sizeof(uint32_t),
            .mv_data = const_cast<uint32_t*>(&keys[i])
        };
        ret = mdb_del(txn, dbi, &mdb_key, /* data= */ nullptr);
        LMDB_CHECK_OK(ret, Put);
    }
    ret = mdb_txn_commit(txn);
    LMDB_CHECK_OK(ret, TxnCommit);
}

std::optional<MDB_dbi> LMDBBackend::GetDB(uint32_t logspace_id) {
    absl::ReaderMutexLock lk(&mu_);
    if (!dbs_.contains(logspace_id)) {
        return std::nullopt;
    }
    return dbs_.at(logspace_id);
}

TkrzwDBMBackend::TkrzwDBMBackend(Type type, std::string_view db_path)
    : type_(type),
      db_path_(db_path) {}

TkrzwDBMBackend::~TkrzwDBMBackend() {
    for (const auto& [logspace_id, dbm] : dbs_) {
        auto status = dbm->Close();
        TKRZW_CHECK_OK(status, Close);
    }
}

void TkrzwDBMBackend::InstallLogSpace(uint32_t logspace_id) {
    HLOG_F(INFO, "Install log space {}", bits::HexStr0x(logspace_id));
    tkrzw::DBM* db_ptr = nullptr;
    if (type_ == kHashDBM) {
        tkrzw::HashDBM* db = new tkrzw::HashDBM();
        tkrzw::HashDBM::TuningParameters params;
        auto status = db->OpenAdvanced(
            /* path= */ fmt::format("{}/{}.tkh", db_path_, bits::HexStr(logspace_id)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ params);
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else if (type_ == kTreeDBM) {
        tkrzw::TreeDBM* db = new tkrzw::TreeDBM();
        tkrzw::TreeDBM::TuningParameters params;
        auto status = db->OpenAdvanced(
            /* path= */ fmt::format("{}/{}.tkt", db_path_, bits::HexStr(logspace_id)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ params);
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else if (type_ == kSkipDBM) {
        tkrzw::SkipDBM* db = new tkrzw::SkipDBM();
        tkrzw::SkipDBM::TuningParameters params;
        auto status = db->OpenAdvanced(
            /* path= */ fmt::format("{}/{}.tks", db_path_, bits::HexStr(logspace_id)),
            /* writable= */ true,
            /* options= */ tkrzw::File::OPEN_DEFAULT,
            /* tuning_params= */ params);
        TKRZW_CHECK_OK(status, Open);
        db_ptr = db;
    } else {
        UNREACHABLE();
    }

    {
        absl::MutexLock lk(&mu_);
        DCHECK(!dbs_.contains(logspace_id));
        dbs_[logspace_id].reset(DCHECK_NOTNULL(db_ptr));
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
