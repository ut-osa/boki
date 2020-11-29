#pragma once

#include "log/common.h"

#include <rocksdb/db.h>

namespace faas {
namespace log {

class StorageInterface {
public:
    virtual ~StorageInterface() {}

    virtual void Add(std::unique_ptr<LogEntry> log_entry) = 0;
    virtual bool Read(uint64_t log_seqnum, std::string* data) = 0;
};

class InMemoryStorage final : public StorageInterface {
public:
    InMemoryStorage();
    ~InMemoryStorage();

    void Add(std::unique_ptr<LogEntry> log_entry) override;
    bool Read(uint64_t log_seqnum, std::string* data) override;

private:
    absl::Mutex mu_;
    absl::flat_hash_map</* seqnum */ uint64_t, std::unique_ptr<LogEntry>>
        entries_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorage);
};

class RocksDBStorage final : public StorageInterface {
public:
    explicit RocksDBStorage(std::string_view db_path);
    ~RocksDBStorage();

    void Add(std::unique_ptr<LogEntry> log_entry) override;
    bool Read(uint64_t log_seqnum, std::string* data) override;

private:
    std::unique_ptr<rocksdb::DB> db_;

    static std::string seqnum_to_key(uint64_t seqnum);

    DISALLOW_COPY_AND_ASSIGN(RocksDBStorage);
};

}  // namespace log
}  // namespace faas
