#pragma once

#include "log/common.h"
#include "log/view.h"

namespace faas {
namespace log {

class Storage {
public:
    Storage();
    ~Storage();

    void Add(std::unique_ptr<LogEntry> log_entry);
    bool Read(uint64_t log_seqnum, std::span<const char>* data);

private:
    absl::flat_hash_map</* seqnum */ uint64_t, std::unique_ptr<LogEntry>> entries_;

    DISALLOW_COPY_AND_ASSIGN(Storage);
};

}  // namespace log
}  // namespace faas
