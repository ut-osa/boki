#pragma once

#include "log/common.h"

namespace faas {
namespace log {

class StorageInterface {
public:
    virtual ~StorageInterface() {}

    virtual void Add(std::unique_ptr<LogEntry> log_entry) = 0;
    virtual bool Read(uint64_t log_seqnum, std::span<const char>* data) = 0;
};

class InMemoryStorage final : public StorageInterface {
public:
    InMemoryStorage();
    ~InMemoryStorage();

    void Add(std::unique_ptr<LogEntry> log_entry) override;
    bool Read(uint64_t log_seqnum, std::span<const char>* data) override;

private:
    absl::Mutex mu_;
    absl::flat_hash_map</* seqnum */ uint64_t, std::unique_ptr<LogEntry>>
        entries_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(InMemoryStorage);
};

}  // namespace log
}  // namespace faas
