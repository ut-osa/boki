#pragma once

#include "log/common.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <rocksdb/db.h>
#include <tkrzw_dbm.h>
#include <lmdb.h>
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace log {

class DBInterface {
public:
    virtual ~DBInterface() {}

    virtual void InstallLogSpace(uint32_t logspace_id) = 0;
    virtual std::optional<std::string> Get(uint32_t logspace_id, uint32_t key) = 0;

    struct Batch {
        uint32_t logspace_id;
        std::vector<uint32_t>    keys;
        std::vector<std::string> data;
    };
    virtual void PutBatch(const Batch& batch) = 0;
};

class RocksDBBackend final : public DBInterface {
public:
    explicit RocksDBBackend(std::string_view db_path);
    ~RocksDBBackend();

    void InstallLogSpace(uint32_t logspace_id) override;
    std::optional<std::string> Get(uint32_t logspace_id, uint32_t key) override;
    void PutBatch(const Batch& batch) override;

private:
    std::unique_ptr<rocksdb::DB> db_;
    absl::Mutex mu_;
    absl::flat_hash_map</* logspace_id */ uint32_t,
                        std::unique_ptr<rocksdb::ColumnFamilyHandle>>
        column_families_ ABSL_GUARDED_BY(mu_);

    rocksdb::ColumnFamilyHandle* GetCFHandle(uint32_t logspace_id);

    DISALLOW_COPY_AND_ASSIGN(RocksDBBackend);
};

class LMDBBackend final : public DBInterface {
public:
    explicit LMDBBackend(std::string_view db_path);
    ~LMDBBackend();

    void InstallLogSpace(uint32_t logspace_id) override;
    std::optional<std::string> Get(uint32_t logspace_id, uint32_t key) override;
    void PutBatch(const Batch& batch) override;

private:
    std::string db_path_;

    MDB_env* env_;

    absl::Mutex mu_;
    absl::flat_hash_map</* logspace_id */ uint32_t, MDB_dbi>
        dbs_ ABSL_GUARDED_BY(mu_);

    std::optional<MDB_dbi> GetDB(uint32_t logspace_id);

    DISALLOW_COPY_AND_ASSIGN(LMDBBackend);
};

class TkrzwDBMBackend final : public DBInterface {
public:
    enum Type { kHashDBM, kTreeDBM, kSkipDBM };
    TkrzwDBMBackend(Type type, std::string_view db_path);
    ~TkrzwDBMBackend();

    void InstallLogSpace(uint32_t logspace_id) override;
    std::optional<std::string> Get(uint32_t logspace_id, uint32_t key) override;
    void PutBatch(const Batch& batch) override;

private:
    Type type_;
    std::string db_path_;

    absl::Mutex mu_;
    absl::flat_hash_map</* logspace_id */ uint32_t,
                        std::unique_ptr<tkrzw::DBM>>
        dbs_ ABSL_GUARDED_BY(mu_);

    tkrzw::DBM* GetDBM(uint32_t logspace_id);

    DISALLOW_COPY_AND_ASSIGN(TkrzwDBMBackend);
};

}  // namespace log
}  // namespace faas
