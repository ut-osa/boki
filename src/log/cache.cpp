#include "log/cache.h"

#include "utils/bits.h"

#include <tkrzw_dbm_cache.h>

namespace faas {
namespace log {

LRUCache::LRUCache(int mem_cap_mb) {
    int64_t cap_mem_size = -1;
    if (mem_cap_mb > 0) {
        cap_mem_size = int64_t{mem_cap_mb} << 20;
    }
    dbm_.reset(new tkrzw::CacheDBM(/* cap_rec_num= */ -1, cap_mem_size));
}

LRUCache::~LRUCache() {}

namespace {
static inline std::string EncodeLogEntry(const LogEntry& log_entry) {
    std::string encoded;
    size_t data_size = log_entry.data.size();
    encoded.resize(data_size + sizeof(LogMetaData));
    if (data_size > 0) {
        memcpy(encoded.data(), log_entry.data.data(), data_size);
    }
    memcpy(encoded.data() + data_size, &log_entry.metadata, sizeof(LogMetaData));
    return encoded;
}

static inline void DecodeLogEntry(std::string&& encoded, LogEntry* log_entry) {
    DCHECK_GE(encoded.size(), sizeof(LogMetaData));
    size_t data_size = encoded.size() - sizeof(LogMetaData);
    memcpy(&log_entry->metadata, encoded.data() + data_size, sizeof(LogMetaData));
    if (data_size > 0) {
        log_entry->data = std::move(encoded);
        DCHECK_GT(log_entry->data.size(), data_size);
        log_entry->data.resize(data_size);
    } else {
        log_entry->data.clear();
    }
}
}  // namespace

void LRUCache::Put(const LogEntry& log_entry) {
    std::string key_str = bits::HexStr(log_entry.metadata.seqnum);
    std::string data = EncodeLogEntry(log_entry);
    dbm_->Set(key_str, data, /* overwrite= */ false);
}

bool LRUCache::Get(uint64_t seqnum, LogEntry* log_entry) {
    std::string key_str = bits::HexStr(seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        DecodeLogEntry(std::move(data), log_entry);
        return true;
    } else {
        return false;
    }
}

}  // namespace log
}  // namespace faas
