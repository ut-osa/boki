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

class StorageTagIndex {
public:
    StorageTagIndex();
    ~StorageTagIndex();

    void Add(uint64_t tag, uint64_t seqnum);
    bool ReadFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                   uint64_t* seqnum) const;
    bool ReadLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                  uint64_t* seqnum) const;

private:
    absl::flat_hash_map</* tag */ uint64_t,
                        std::unique_ptr<std::set</* seqnum */ uint64_t>>> indices_;

    DISALLOW_COPY_AND_ASSIGN(StorageTagIndex);
};

class InMemoryStorage final : public StorageInterface {
public:
    InMemoryStorage();
    ~InMemoryStorage();

    void Add(std::unique_ptr<LogEntry> log_entry) override;
    bool Read(uint64_t seqnum, std::string* data) override;

    bool ReadFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                   uint64_t* seqnum, std::string* data) override;
    bool ReadLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                  uint64_t* seqnum, std::string* data) override;

private:
    absl::Mutex mu_;
    StorageTagIndex tag_index_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* seqnum */ uint64_t, std::unique_ptr<LogEntry>>
        entries_ ABSL_GUARDED_BY(mu_);

    bool ReadInternal(uint64_t seqnum, std::string* data) const
        ABSL_SHARED_LOCKS_REQUIRED(mu_);

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorage);
};

class RocksDBStorage final : public StorageInterface {
public:
    explicit RocksDBStorage(std::string_view db_path);
    ~RocksDBStorage();

    void Add(std::unique_ptr<LogEntry> log_entry) override;
    bool Read(uint64_t seqnum, std::string* data) override;

    bool ReadFirst(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                   uint64_t* seqnum, std::string* data) override;
    bool ReadLast(uint64_t tag, uint64_t start_seqnum, uint64_t end_seqnum,
                  uint64_t* seqnum, std::string* data) override;

private:
    absl::Mutex mu_;
    StorageTagIndex tag_index_ ABSL_GUARDED_BY(mu_);

    std::unique_ptr<rocksdb::DB> db_;

    static std::string seqnum_to_key(uint64_t seqnum);

    DISALLOW_COPY_AND_ASSIGN(RocksDBStorage);
};

}  // namespace log
}  // namespace faas
