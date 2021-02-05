#pragma once

#include "log/common.h"
#include "log/view.h"

namespace faas {
namespace log_utils {

// Used for on-holding requests for future views
class FutureRequests {
public:
    FutureRequests();
    ~FutureRequests();

    // Both `OnNewView` and `OnHoldRequest` are thread safe

    // If `ready_requests` is nullptr, will panic if there are on-hold requests
    void OnNewView(const log::View* view,
                   std::vector<log::SharedLogRequest>* ready_requests);
    void OnHoldRequest(uint16_t view_id, log::SharedLogRequest request);

private:
    absl::Mutex mu_;

    uint16_t next_view_id_ ABSL_GUARDED_BY(mu_);
    absl::flat_hash_map</* view_id */ uint16_t, std::vector<log::SharedLogRequest>>
        onhold_requests_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(FutureRequests);
};

template<class T>
class ThreadedMap {
public:
    ThreadedMap();
    ~ThreadedMap();

    // All these APIs are thread safe
    void Put(uint64_t key, T* value);         // Override if the given key exists
    bool Poll(uint64_t key, T** value);       // Remove the given key if it is found
    void PutChecked(uint64_t key, T* value);  // Panic if key exists
    T*   PollChecked(uint64_t key);           // Panic if key does not exist
    void RemoveChecked(uint64_t key);         // Panic if key does not exist
    void PollAll(std::vector<std::pair<uint64_t, T*>>* values);
    void PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values);

private:
    absl::Mutex mu_;
    absl::flat_hash_map<uint64_t, T*> rep_ ABSL_GUARDED_BY(mu_);

    DISALLOW_COPY_AND_ASSIGN(ThreadedMap);
};

log::MetaLogsProto MetaLogsFromPayload(std::span<const char> payload);

log::LogMetaData GetMetaDataFromMessage(const protocol::SharedLogMessage& message);
void SplitPayloadForMessage(const protocol::SharedLogMessage& message,
                            std::span<const char> payload,
                            std::span<const uint64_t>* user_tags,
                            std::span<const char>* log_data,
                            std::span<const char>* aux_data);

void PopulateMetaDataToMessage(const log::LogMetaData& metadata,
                               protocol::SharedLogMessage* message);
void PopulateMetaDataToMessage(const log::LogEntryProto& log_entry,
                               protocol::SharedLogMessage* message);

// Start implementation of ThreadedMap

template<class T>
ThreadedMap<T>::ThreadedMap() {}

template<class T>
ThreadedMap<T>::~ThreadedMap() {
#if DCHECK_IS_ON()
    if (!rep_.empty()) {
        LOG(WARNING) << fmt::format("There are {} elements left", rep_.size());
    }
#endif
}

template<class T>
void ThreadedMap<T>::Put(uint64_t key, T* value) {
    absl::MutexLock lk(&mu_);
    rep_[key] = value;
}

template<class T>
bool ThreadedMap<T>::Poll(uint64_t key, T** value) {
    absl::MutexLock lk(&mu_);
    if (rep_.contains(key)) {
        *value = rep_.at(key);
        rep_.erase(key);
        return true;
    } else {
        return false;
    }
}

template<class T>
void ThreadedMap<T>::PutChecked(uint64_t key, T* value) {
    absl::MutexLock lk(&mu_);
    DCHECK(!rep_.contains(key));
    rep_[key] = value;
}

template<class T>
T* ThreadedMap<T>::PollChecked(uint64_t key) {
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key));
    T* value = rep_.at(key);
    rep_.erase(key);
    return value;
}

template<class T>
void ThreadedMap<T>::RemoveChecked(uint64_t key) {
    absl::MutexLock lk(&mu_);
    DCHECK(rep_.contains(key));
    rep_.erase(key);
}

template<class T>
void ThreadedMap<T>::PollAll(std::vector<std::pair<uint64_t, T*>>* values) {
    absl::MutexLock lk(&mu_);
    values->resize(rep_.size());
    if (values->empty()) {
        return;
    }
    size_t i = 0;
    for (const auto& [key, value] : rep_) {
        (*values)[i++] = std::make_pair(key, value);
    }
    DCHECK_EQ(i, rep_.size());
    rep_.clear();
}

template<class T>
void ThreadedMap<T>::PollAllSorted(std::vector<std::pair<uint64_t, T*>>* values) {
    PollAll(values);
    if (values->empty()) {
        return;
    }
    std::sort(
        values->begin(), values->end(),
        [] (const std::pair<uint64_t, T*>& lhs, const std::pair<uint64_t, T*>& rhs) -> bool {
            return lhs.first < rhs.first;
        }
    );
}

}  // namespace log_utils
}  // namespace faas
