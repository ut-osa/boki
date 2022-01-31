#include "log/db_workers.h"

#include "log/storage_base.h"

namespace faas {
namespace log {

DBWorkers::DBWorkers(StorageBase* storage, size_t num_threads)
    : storage_(storage),
      worker_threads_(num_threads),
      idle_threads_(0) {
    for (size_t i = 0; i < num_threads; i++) {
        worker_threads_[i].emplace(
            fmt::format("BG/{}", i),
            absl::bind_front(&DBWorkers::WorkerThreadMain, this, i));
        worker_threads_[i]->Start();
    }
    flush_cq_.SetCommitFn([storage] (std::span<const LogStorage::Entry* const> entries) {
        storage->CommitLogEntries(entries);
    });
}

void DBWorkers::SubmitLogEntriesForFlush(std::span<const LogStorage::Entry* const> entries) {
    absl::MutexLock lk(&mu_);
    flush_sq_.Submit(entries);
    if (idle_threads_ > 0) {
        cv_.Signal();
    }
}

void DBWorkers::SubmitSeqnumsForTrim(std::span<const uint64_t> seqnums) {
    absl::MutexLock lk(&mu_);
    trim_sq_.Submit(seqnums);
    if (idle_threads_ > 0) {
        cv_.Signal();
    }
}

void DBWorkers::SignalAllThreads() {
    cv_.SignalAll();
}

void DBWorkers::JoinAllThreads() {
    for (size_t i = 0; i < worker_threads_.size(); i++) {
        worker_threads_[i]->Join();
    }
}

void DBWorkers::WorkerThreadMain(int thread_index) {
    std::vector<const LogStorage::Entry*> log_entries;
    std::vector<uint64_t> seqnums;
    uint64_t starting_id;

    while (storage_->running()) {
        mu_.Lock();
        while (flush_sq_.empty() && trim_sq_.empty()) {
            idle_threads_++;
            VLOG(1) << "Nothing to do, will sleep to wait";
            cv_.Wait(&mu_);
            if (!storage_->running()) {
                mu_.Unlock();
                return;
            }
            idle_threads_--;
            DCHECK_GE(idle_threads_, 0);
        }
        if (!flush_sq_.empty()) {
            flush_sq_.Pull(kBatchSize, &log_entries, &starting_id);
        } else if (!trim_sq_.empty()) {
            trim_sq_.Pull(kBatchSize, &seqnums, &starting_id);
        } else {
            UNREACHABLE();
        }
        mu_.Unlock();
        if (!log_entries.empty()) {
            storage_->FlushLogEntries(VECTOR_AS_SPAN(log_entries));
            flush_cq_.Push(VECTOR_AS_SPAN(log_entries), starting_id);
            log_entries.clear();
        } else if (!seqnums.empty()) {
            storage_->TrimLogEntries(VECTOR_AS_SPAN(seqnums));
            seqnums.clear();
        } else {
            UNREACHABLE();
        }
    }
    mu_.AssertNotHeld();
}

}  // namespace log
}  // namespace faas
