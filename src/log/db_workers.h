#pragma once

#include "log/common.h"
#include "log/log_space.h"

namespace faas {
namespace log {

class StorageBase;

class DBWorkers {
public:
    DBWorkers(StorageBase* storage, size_t num_worker_threads);
    ~DBWorkers();

    void PushLogEntriesForFlush(std::span<const LogStorage::Entry*> entries);
    void PushSeqnumsForTrim(std::span<const uint64_t> seqnums);

    void SignalAllThreads();
    void JoinAllThreads();

private:
    static constexpr size_t kBatchSize = 128;

    StorageBase* storage_;
    absl::FixedArray<std::optional<base::Thread>> worker_threads_;

    absl::Mutex mu_;
    absl::CondVar cv_;

    int idle_workers_ ABSL_GUARDED_BY(mu_);
    std::deque<std::pair</* seqnum */ uint64_t, const LogStorage::Entry*>>
        queue_ ABSL_GUARDED_BY(mu_);
    uint64_t next_seqnum_ ABSL_GUARDED_BY(mu_);

    absl::Mutex commit_mu_;
    uint64_t commited_seqnum_    ABSL_GUARDED_BY(commit_mu_);
    std::map</* seqnum */ uint64_t, const LogStorage::Entry*>
        flushed_entries_         ABSL_GUARDED_BY(commit_mu_);

    stat::StatisticsCollector<int> queue_length_stat_ ABSL_GUARDED_BY(mu_);

    void WorkerThreadMain(int thread_index);

    DISALLOW_COPY_AND_ASSIGN(DBWorkers);
};

}  // namespace log
}  // namespace faas
