#pragma once

#include "log/common.h"

#include <rocksdb/db.h>

namespace faas {
namespace log {

class StorageInterface {
public:
    virtual ~StorageInterface() {}

    virtual void Add(std::unique_ptr<LogEntry> log_entry) = 0;
    virtual bool Read(uint64_t seqnum, std::string* data) = 0;

    // Read the first log with given tag such that seqnum in [start, end)
    virtual bool ReadFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                           uint64_t* seqnum, std::string* data) = 0;
    // Read the last log with given tag such that seqnum in [start, end)
    virtual bool ReadLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                          uint64_t* seqnum, std::string* data) = 0;
};

class RocksDBStorage {
public:
    explicit RocksDBStorage(std::string_view db_path);
    ~RocksDBStorage();

    void Add(uint64_t seqnum, std::span<const char> data);
    bool Read(uint64_t seqnum, std::string* data);

private:
    std::unique_ptr<rocksdb::DB> db_;
    DISALLOW_COPY_AND_ASSIGN(RocksDBStorage);
};

}  // namespace log
}  // namespace faas
