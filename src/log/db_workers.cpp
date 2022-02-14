#include "log/db_workers.h"

#include "log/storage_base.h"

ABSL_FLAG(size_t, db_workers_max_batch_size, 256, "");

namespace faas {
namespace log {

DBWorkers::DBWorkers(StorageBase* storage, size_t num_threads)
    : max_batch_size_(absl::GetFlag(FLAGS_db_workers_max_batch_size)),
      storage_(storage),
      worker_threads_(num_threads),
      idle_threads_(0),
      gc_scheduled_(false),
      flush_queue_length_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          "db_flush_queue_length"), "storage"),
      batch_size_stat_(stat::StatisticsCollector<int>::StandardReportCallback(
          "db_workers_batch_size"), "storage") {
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

void DBWorkers::ScheduleGC() {
    absl::MutexLock lk(&mu_);
    if (!gc_scheduled_) {
        gc_scheduled_ = true;
        if (idle_threads_ > 0) {
            cv_.Signal();
        }
    }
}

void DBWorkers::SubmitLogEntriesForFlush(std::span<const LogStorage::Entry* const> entries) {
    absl::MutexLock lk(&mu_);
    flush_sq_.Submit(entries);
    flush_queue_length_stat_.AddSample(gsl::narrow_cast<int>(flush_sq_.size()));
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
    bool run_gc = false;
    uint64_t starting_id;

    while (storage_->running()) {
        mu_.Lock();
        while (!gc_scheduled_ && flush_sq_.empty() && trim_sq_.empty()) {
            idle_threads_++;
            cv_.Wait(&mu_);
            if (!storage_->running()) {
                mu_.Unlock();
                return;
            }
            idle_threads_--;
            DCHECK_GE(idle_threads_, 0);
        }
        if (gc_scheduled_) {
            gc_scheduled_ = false;
            run_gc = true;
        } else if (!flush_sq_.empty()) {
            flush_sq_.Pull(max_batch_size_, &log_entries, &starting_id);
            batch_size_stat_.AddSample(gsl::narrow_cast<int>(log_entries.size()));
        } else if (!trim_sq_.empty()) {
            trim_sq_.Pull(max_batch_size_, &seqnums, &starting_id);
        } else {
            UNREACHABLE();
        }
        mu_.Unlock();
        if (run_gc) {
            storage_->CollectLogTrimOps();
            run_gc = false;
        } else if (!log_entries.empty()) {
            storage_->DBFlushLogEntries(VECTOR_AS_SPAN(log_entries));
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
