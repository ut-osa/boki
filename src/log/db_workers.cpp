#include "log/db_workers.h"

#include "log/storage_base.h"

namespace faas {
namespace log {

DBWorkers::DBWorkers(StorageBase* storage, size_t num_worker_threads)
    : storage_(storage),
      worker_threads_(num_worker_threads),
      idle_workers_(0),
      next_seqnum_(0),
      commited_seqnum_(0),
      queue_length_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          "flush_queue_length")) {
    for (size_t i = 0; i < num_worker_threads; i++) {
        worker_threads_[i].emplace(
            fmt::format("BG/{}", i),
            absl::bind_front(&DBWorkers::WorkerThreadMain, this, i));
        worker_threads_[i]->Start();
    }
}

DBWorkers::~DBWorkers() {}

void DBWorkers::PushLogEntriesForFlush(std::span<const LogStorage::Entry*> entries) {
    DCHECK(!entries.empty());
    absl::MutexLock lk(&mu_);
    for (const LogStorage::Entry* entry : entries) {
        queue_.push_back(std::make_pair(next_seqnum_++, entry));
    }
    queue_length_stat_.AddSample(gsl::narrow_cast<int>(queue_.size()));
    if (idle_workers_ > 0) {
        cv_.Signal();
    }
}

void DBWorkers::PushSeqnumsForTrim(std::span<const uint64_t> seqnums) {
    NOT_IMPLEMENTED();
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
    std::vector<const LogStorage::Entry*> entries;
    entries.reserve(kBatchSize);
    while (storage_->running()) {
        mu_.Lock();
        while (queue_.empty()) {
            idle_workers_++;
            VLOG(1) << "Nothing to flush, will sleep to wait";
            cv_.Wait(&mu_);
            if (!storage_->running()) {
                mu_.Unlock();
                return;
            }
            idle_workers_--;
            DCHECK_GE(idle_workers_, 0);
        }
        uint64_t start_seqnum = queue_.front().first;
        uint64_t end_seqnum = start_seqnum;
        entries.clear();
        while (entries.size() < kBatchSize && !queue_.empty()) {
            end_seqnum = queue_.front().first + 1;
            entries.push_back(queue_.front().second);
            queue_.pop_front();
        }
        mu_.Unlock();

        DCHECK_LT(start_seqnum, end_seqnum);
        DCHECK_EQ(static_cast<size_t>(end_seqnum - start_seqnum), entries.size());
        DCHECK_LE(entries.size(), kBatchSize);
        storage_->FlushLogEntries(
            std::span<const LogStorage::Entry*>(entries.data(), entries.size()));
        {
            absl::MutexLock lk(&commit_mu_);
            for (size_t i = 0; i < entries.size(); i++) {
                uint64_t seqnum = start_seqnum + static_cast<uint64_t>(i);
                flushed_entries_[seqnum] = entries[i];
            }
            entries.clear();
            auto iter = flushed_entries_.begin();
            while (iter->first == commited_seqnum_) {
                entries.push_back(iter->second);
                iter = flushed_entries_.erase(iter);
                commited_seqnum_++;
            }
            storage_->CommitLogEntries(
                std::span<const LogStorage::Entry*>(entries.data(), entries.size()));
        }
    }
    mu_.AssertNotHeld();
}

}  // namespace log
}  // namespace faas
