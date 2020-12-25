#include "log/storage.h"

#include "log/flags.h"

namespace faas {
namespace log {

RocksDBStorage::RocksDBStorage(std::string_view db_path) {
    rocksdb::Options options;
    options.create_if_missing = true;
    rocksdb::DB* db;
    LOG(INFO) << "Open RocksDB at path " << db_path;
    auto status = rocksdb::DB::Open(options, std::string(db_path), &db);
    if (!status.ok()) {
        LOG(FATAL) << "RocksDB open failed: " << status.ToString();
    }
    db_.reset(db);
}

RocksDBStorage::~RocksDBStorage() {}

void RocksDBStorage::Add(uint64_t seqnum, std::span<const char> data) {
    auto status = db_->Put(rocksdb::WriteOptions(),
                           /* key= */   SeqNumHexStr(seqnum),
                           /* value= */ rocksdb::Slice(data.data(), data.size()));
    if (!status.ok()) {
        LOG(FATAL) << "RocksDB put failed: " << status.ToString();
    }
}

bool RocksDBStorage::Read(uint64_t seqnum, std::string* data) {
    auto status = db_->Get(rocksdb::ReadOptions(),
                           /* key= */   SeqNumHexStr(seqnum),
                           /* value= */ data);
    if (!status.ok() && !status.IsNotFound()) {
        LOG(FATAL) << "RocksDB get failed: " << status.ToString();
    }
    return status.ok();
}

}  // namespace log
}  // namespace faas
