#pragma once

#include "log/common.h"

// Forward declarations
namespace rocksdb { class DB; class ColumnFamilyHandle; }
namespace tkrzw { class DBM; }

namespace faas {
namespace log {

class DBInterface {
public:
    virtual ~DBInterface() {}

    virtual void InstallLogSpace(uint32_t logspace_id) = 0;
    virtual std::optional<std::string> Get(uint32_t logspace_id, uint32_t key) = 0;
    virtual void Put(uint32_t logspace_id, uint32_t key, std::span<const char> data) = 0;
};

class RocksDBBackend final : public DBInterface {
public:
    explicit RocksDBBackend(std::string_view db_path);
    ~RocksDBBackend();

    void InstallLogSpace(uint32_t logspace_id) override;
    std::optional<std::string> Get(uint32_t logspace_id, uint32_t key) override;
    void Put(uint32_t logspace_id, uint32_t key, std::span<const char> data) override;

private:
    std::unique_ptr<rocksdb::DB> db_;
    absl::Mutex mu_;
    absl::flat_hash_map</* logspace_id */ uint32_t,
                        std::unique_ptr<rocksdb::ColumnFamilyHandle>>
        column_families_ ABSL_GUARDED_BY(mu_);

    rocksdb::ColumnFamilyHandle* GetCFHandle(uint32_t logspace_id);

    DISALLOW_COPY_AND_ASSIGN(RocksDBBackend);
};

class TkrzwDBMBackend final : public DBInterface {
public:
    enum Type { kHashDBM, kTreeDBM, kSkipDBM };
    TkrzwDBMBackend(Type type, std::string_view db_path);
    ~TkrzwDBMBackend();

    void InstallLogSpace(uint32_t logspace_id) override;
    std::optional<std::string> Get(uint32_t logspace_id, uint32_t key) override;
    void Put(uint32_t logspace_id, uint32_t key, std::span<const char> data) override;

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
