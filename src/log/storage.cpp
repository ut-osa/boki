#include "log/storage.h"

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

bool InMemoryStorage::Read(uint64_t log_seqnum, std::span<const char>* data) {
    absl::MutexLock lk(&mu_);
    if (!entries_.contains(log_seqnum)) {
        return false;
    }
    LogEntry* entry = entries_[log_seqnum].get();
    *data = std::span<const char>(entry->data.data(), entry->data.size());
    return true;
}

}  // namespace log
}  // namespace faas
