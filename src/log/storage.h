#pragma once

#include "log/common.h"

#include <rocksdb/db.h>

namespace faas {
namespace log {

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
