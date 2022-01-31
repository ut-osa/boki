#pragma once

#include "base/common.h"

namespace faas {
namespace utils {

template<class T>
class RoundRobinSet {
public:
    RoundRobinSet();
    ~RoundRobinSet() = default;

    size_t size() const { return elems_.size(); }

    bool Add(const T& elem);
    bool Remove(const T& elem);
    bool PickNext(T* elem);

private:
    std::set<T> elems_;
    typename std::set<T>::iterator current_;
    DISALLOW_COPY_AND_ASSIGN(RoundRobinSet);
};

template<class T>
RoundRobinSet<T>::RoundRobinSet()
    : current_(elems_.begin()) {}

template<class T>
bool RoundRobinSet<T>::Add(const T& elem) {
    auto ret = elems_.insert(elem);
    return ret.second;
}

template<class T>
bool RoundRobinSet<T>::Remove(const T& elem) {
    auto iter = elems_.find(elem);
    if (iter == elems_.end()) {
        return false;
    }
    if (iter == current_) {
        current_ = elems_.erase(iter);
    } else {
        elems_.erase(iter);
    }
    return true;
}

template<class T>
bool RoundRobinSet<T>::PickNext(T* elem) {
    if (elems_.empty()) {
        return false;
    }
    if (current_ == elems_.end()) {
        current_ = elems_.begin();
    }
    *elem = *current_;
    current_++;
    return true;
}

}  // namespace utils
}  // namespace faas
