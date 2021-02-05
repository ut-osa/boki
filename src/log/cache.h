#pragma once

#include "log/common.h"

// Forward declarations
namespace tkrzw { class CacheDBM; }

namespace faas {
namespace log {

class LRUCache {
public:
    explicit LRUCache(int mem_cap_mb);
    ~LRUCache();

    void Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
             std::span<const char> log_data);
    bool Get(uint64_t seqnum, LogEntry* log_entry);

    void PutAuxData(uint64_t seqnum, std::span<const char> data);
    bool GetAuxData(uint64_t seqnum, std::string* data);

private:
    std::unique_ptr<tkrzw::CacheDBM> dbm_;

    DISALLOW_COPY_AND_ASSIGN(LRUCache);
};

}  // namespace log
}  // namespace faas
