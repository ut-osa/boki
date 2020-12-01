#include "log/storage.h"

#include "log/flags.h"

namespace faas {
namespace log {

InMemoryStorage::InMemoryStorage() {}

InMemoryStorage::~InMemoryStorage() {}

void InMemoryStorage::Add(std::unique_ptr<LogEntry> log_entry) {
    VLOG(1) << fmt::format("Storing log (localid={}, seqnum={})",
                           log_entry->localid, log_entry->seqnum);
    uint64_t seqnum = log_entry->seqnum;
    absl::MutexLock lk(&mu_);
    DCHECK(!entries_.contains(seqnum));
    entries_[seqnum] = std::move(log_entry);
}

bool InMemoryStorage::Read(uint64_t log_seqnum, std::string* data) {
    absl::MutexLock lk(&mu_);
    if (!entries_.contains(log_seqnum)) {
        return false;
    }
    LogEntry* entry = entries_[log_seqnum].get();
    *data = entry->data;
    return true;
}

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

void RocksDBStorage::Add(std::unique_ptr<LogEntry> log_entry) {
    auto status = db_->Put(rocksdb::WriteOptions(),
                           /* key= */ SeqNumHexStr(log_entry->seqnum),
                           /* value= */ log_entry->data);
    if (!status.ok()) {
        LOG(FATAL) << "RocksDB put failed: " << status.ToString();
    }
}

bool RocksDBStorage::Read(uint64_t log_seqnum, std::string* data) {
    auto status = db_->Get(rocksdb::ReadOptions(),
                           /* key= */ SeqNumHexStr(log_seqnum),
                           /* value= */ data);
    if (!status.ok() && !status.IsNotFound()) {
        LOG(FATAL) << "RocksDB get failed: " << status.ToString();
    }
    return status.ok();
}

}  // namespace log
}  // namespace faas
