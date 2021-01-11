namespace faas {
namespace log {

template<class T>
LogSpaceCollection<T>::LogSpaceCollection()
    : next_view_id_(0) {}

template<class T>
LogSpaceCollection<T>::~LogSpaceCollection() {}

template<class T>
LockablePtr<T> LogSpaceCollection<T>::GetLogSpace(uint32_t identifier) const {
    if (log_spaces_.contains(identifier)) {
        return log_spaces_.at(identifier);
    } else {
        return LockablePtr<T>{};
    }
}

template<class T>
void LogSpaceCollection<T>::OnNewView(const View* view,
                                      std::vector<std::unique_ptr<T>> log_spaces) {
    if (view->id() != next_view_id_) {
        LOG(FATAL) << fmt::format("Views are not consecutive: have={}, expect={}",
                                  view->id(), next_view_id_);
    }
    for (std::unique_ptr<T>& log_space : log_spaces) {
        DCHECK_EQ(log_space->view_id(), view->id());
        uint32_t identifier = log_space->identifier();
        DCHECK(active_log_spaces_.count(identifier) == 0);
        active_log_spaces_.insert(identifier);
        log_spaces_[identifier] = LockablePtr<T>(std::move(log_space));
    }
    next_view_id_++;
}

template<class T>
bool LogSpaceCollection<T>::FinalizeLogSpace(uint32_t identifier) {
    if (active_log_spaces_.count(identifier) == 0) {
        return false;
    }
    active_log_spaces_.erase(identifier);
    DCHECK(finalized_log_spaces_.count(identifier) == 0);
    finalized_log_spaces_.insert(identifier);
    return true;
}

template<class T>
bool LogSpaceCollection<T>::RemoveLogSpace(uint32_t identifier) {
    if (finalized_log_spaces_.count(identifier) == 0) {
        return false;
    }
    finalized_log_spaces_.erase(identifier);
    DCHECK(log_spaces_.contains(identifier));
    log_spaces_.erase(identifier);
    return true;
}

template<class T>
bool LogSpaceCollection<T>::GetAllActiveLogSpaces(uint16_t view_id,
                                                  std::vector<LockablePtr<T>>* results) const {
    if (view_id >= next_view_id_) {
        return false;
    }
    results->clear();
    auto iter = active_log_spaces_.lower_bound(bits::JoinTwo16(view_id, 0));
    while (iter != active_log_spaces_.end()) {
        if (bits::HighHalf32(*iter) > view_id) {
            break;
        }
        DCHECK(log_spaces_.contains(*iter));
        results->push_back(log_spaces_.at(*iter));
    }
    return !results->empty();
}

template<class T>
LockablePtr<T> LogSpaceCollection<T>::GetNextActiveLogSpace(uint32_t min_identifier) const {
    auto iter = active_log_spaces_.lower_bound(min_identifier);
    if (iter != active_log_spaces_.end()) {
        DCHECK(log_spaces_.contains(*iter));
        return log_spaces_.at(*iter);
    } else {
        return LockablePtr<T>{};
    }
}

template<class T>
LockablePtr<T> LogSpaceCollection<T>::GetNextFinalizedLogSpace(uint32_t min_identifier) const {
    auto iter = finalized_log_spaces_.lower_bound(min_identifier);
    if (iter != finalized_log_spaces_.end()) {
        DCHECK(log_spaces_.contains(*iter));
        return log_spaces_.at(*iter);
    } else {
        return LockablePtr<T>{};
    }
}

}  // namespace log
}  // namespace faas
