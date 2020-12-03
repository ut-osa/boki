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
    if (log_entry->tag != kDefaultLogTag) {
        if (!seqnum_indices_.contains(log_entry->tag)) {
            seqnum_indices_[log_entry->tag] = std::make_unique<std::set<uint64_t>>();
        }
        seqnum_indices_[log_entry->tag]->insert(log_entry->seqnum);
    }
    entries_[seqnum] = std::move(log_entry);
}

bool InMemoryStorage::Read(uint64_t seqnum, std::string* data) {
    absl::ReaderMutexLock lk(&mu_);
    return ReadInternal(seqnum, data);
}

bool InMemoryStorage::ReadFirst(uint32_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                                uint64_t* seqnum, std::string* data) {
    absl::ReaderMutexLock lk(&mu_);
    if (!seqnum_indices_.contains(tag)) {
        return false;
    }
    const std::set<uint64_t>& index = *seqnum_indices_.at(tag).get();
    auto iter = index.lower_bound(start_seqnum);
    if (iter != index.end() && *iter < end_seqnum) {
        *seqnum = *iter;
        DCHECK(ReadInternal(*iter, data));
        return true;
    } else {
        return false;
    }
}

bool InMemoryStorage::ReadLast(uint32_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                               uint64_t* seqnum, std::string* data) {
    absl::ReaderMutexLock lk(&mu_);
    if (!seqnum_indices_.contains(tag)) {
        return false;
    }
    const std::set<uint64_t>& index = *seqnum_indices_.at(tag).get();
    auto iter = index.lower_bound(end_seqnum);
    if (iter != index.begin() && *(--iter) >= start_seqnum) {
        *seqnum = *iter;
        DCHECK(ReadInternal(*iter, data));
        return true;
    } else {
        return false;
    }
}

bool InMemoryStorage::ReadInternal(uint64_t seqnum, std::string* data) const {
    if (!entries_.contains(seqnum)) {
        return false;
    }
    const LogEntry* entry = entries_.at(seqnum).get();
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

bool RocksDBStorage::Read(uint64_t seqnum, std::string* data) {
    auto status = db_->Get(rocksdb::ReadOptions(),
                           /* key= */ SeqNumHexStr(seqnum),
                           /* value= */ data);
    if (!status.ok() && !status.IsNotFound()) {
        LOG(FATAL) << "RocksDB get failed: " << status.ToString();
    }
    return status.ok();
}

bool RocksDBStorage::ReadFirst(uint32_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                               uint64_t* seqnum, std::string* data) {
    LOG(FATAL) << "Not implemented";
}

bool RocksDBStorage::ReadLast(uint32_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                              uint64_t* seqnum, std::string* data) {
    LOG(FATAL) << "Not implemented";
}


}  // namespace log
}  // namespace faas
