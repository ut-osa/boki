#pragma once

#include "log/common.h"
#include "log/log_space.h"

namespace faas {
namespace log {

class StorageBase;

class DBWorkers {
public:
    DBWorkers(StorageBase* storage, size_t num_worker_threads);
    ~DBWorkers() = default;

    void SubmitLogEntriesForFlush(std::span<const LogStorage::Entry* const> entries);
    void SubmitSeqnumsForTrim(std::span<const uint64_t> seqnums);

    void SignalAllThreads();
    void JoinAllThreads();

private:
    template<class T>
    class SubmissionQueue {
    public:
        SubmissionQueue() = default;
        ~SubmissionQueue() = default;

        size_t size() const { return queue_.size(); }
        bool empty() const { return queue_.empty(); }

        void Submit(std::span<const T> entries);
        void Pull(size_t batch_size, std::vector<T>* entries, uint64_t* starting_id);

    private:
        uint64_t front_id_{0};
        uint64_t tail_id_{0};
        std::queue<T> queue_;

        DISALLOW_COPY_AND_ASSIGN(SubmissionQueue);
    };

    template<class T>
    class CompletionQueue {
    public:
        CompletionQueue() = default;
        ~CompletionQueue() = default;

        using CommitFn = std::function<void(std::span<const T>)>;
        void SetCommitFn(CommitFn fn) { commit_fn_ = fn; }

        void Push(std::span<const T> entries, uint64_t starting_id);

    private:
        absl::Mutex mu_;
        CommitFn commit_fn_;
        uint64_t committed_id_position_{0};
        std::map</* id */ uint64_t, T> finished_entries_;
        std::vector<T> buffer_;

        DISALLOW_COPY_AND_ASSIGN(CompletionQueue);
    };

    static constexpr size_t kBatchSize = 128;

    StorageBase* storage_;
    absl::FixedArray<std::optional<base::Thread>> worker_threads_;

    absl::Mutex mu_;
    absl::CondVar cv_;

    int                                       idle_threads_ ABSL_GUARDED_BY(mu_);
    SubmissionQueue<const LogStorage::Entry*> flush_sq_     ABSL_GUARDED_BY(mu_);
    SubmissionQueue</* seqnum */ uint64_t>    trim_sq_      ABSL_GUARDED_BY(mu_);

    CompletionQueue<const LogStorage::Entry*> flush_cq_;

    void WorkerThreadMain(int thread_index);

    DISALLOW_COPY_AND_ASSIGN(DBWorkers);
};

template<class T>
void DBWorkers::SubmissionQueue<T>::Submit(std::span<const T> entries) {
    DCHECK(!entries.empty());
    for (auto entry : entries) {
        tail_id_++;
        queue_.push(entry);
    }
}

template<class T>
void DBWorkers::SubmissionQueue<T>::Pull(size_t batch_size,
                                         std::vector<T>* entries, uint64_t* starting_id) {
    DCHECK(!empty() && batch_size > 0);
    DCHECK(entries != nullptr && entries->empty());
    *DCHECK_NOTNULL(starting_id) = front_id_;
    while (entries->size() < batch_size && !empty()) {
        entries->push_back(queue_.front());
        queue_.pop();
        front_id_++;
    }
}

template<class T>
void DBWorkers::CompletionQueue<T>::Push(std::span<const T> entries, uint64_t starting_id) {
    DCHECK(!entries.empty());
    absl::MutexLock lk(&mu_);
    for (size_t i = 0; i < entries.size(); i++) {
        uint64_t id = starting_id + static_cast<uint64_t>(i);
        finished_entries_[id] = entries[i];
    }
    buffer_.clear();
    auto iter = finished_entries_.begin();
    while (iter != finished_entries_.end() && iter->first == committed_id_position_) {
        buffer_.push_back(iter->second);
        iter = finished_entries_.erase(iter);
        committed_id_position_++;
    }
    if (commit_fn_) {
        commit_fn_(VECTOR_AS_SPAN(buffer_));
    }
}

}  // namespace log
}  // namespace faas
