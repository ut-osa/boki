#include "log/cache.h"

#include "utils/bits.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
__END_THIRD_PARTY_HEADERS

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
static inline std::string EncodeLogEntry(const LogMetaData& log_metadata,
                                         std::span<const uint64_t> user_tags,
                                         std::span<const char> log_data) {
    DCHECK_EQ(log_metadata.num_tags, user_tags.size());
    DCHECK_EQ(log_metadata.data_size, log_data.size());
    size_t total_size = log_data.size()
                      + user_tags.size() * sizeof(uint64_t)
                      + sizeof(LogMetaData);
    std::string encoded;
    encoded.resize(total_size);
    char* ptr = encoded.data();
    DCHECK_GT(log_data.size(), 0U);
    memcpy(ptr, log_data.data(), log_data.size());
    ptr += log_data.size();
    if (user_tags.size() > 0) {
        memcpy(ptr, user_tags.data(), user_tags.size() * sizeof(uint64_t));
        ptr += user_tags.size() * sizeof(uint64_t);
    }
    memcpy(ptr, &log_metadata, sizeof(LogMetaData));
    return encoded;
}

static inline void DecodeLogEntry(std::string encoded, LogEntry* log_entry) {
    DCHECK_GT(encoded.size(), sizeof(LogMetaData));
    LogMetaData& metadata = log_entry->metadata;
    memcpy(&metadata,
           encoded.data() + encoded.size() - sizeof(LogMetaData),
           sizeof(LogMetaData));
    size_t total_size = metadata.data_size
                      + metadata.num_tags * sizeof(uint64_t)
                      + sizeof(LogMetaData);
    DCHECK_EQ(total_size, encoded.size());
    if (metadata.num_tags > 0) {
        std::span<const uint64_t> user_tags(
            reinterpret_cast<const uint64_t*>(encoded.data() + metadata.data_size),
            metadata.num_tags);
        log_entry->user_tags.assign(user_tags.begin(), user_tags.end());
    } else {
        log_entry->user_tags.clear();
    }
    encoded.resize(metadata.data_size);
    log_entry->data = std::move(encoded);
}
}  // namespace

void LRUCache::Put(const LogMetaData& log_metadata, std::span<const uint64_t> user_tags,
                   std::span<const char> log_data) {
    std::string key_str = bits::HexStr(log_metadata.seqnum);
    std::string data = EncodeLogEntry(log_metadata, user_tags, log_data);
    dbm_->Set(key_str, data, /* overwrite= */ false);
}

bool LRUCache::Get(uint64_t seqnum, LogEntry* log_entry) {
    std::string key_str = bits::HexStr(seqnum);
    std::string data;
    auto status = dbm_->Get(key_str, &data);
    if (status.IsOK()) {
        DecodeLogEntry(std::move(data), log_entry);
        DCHECK_EQ(seqnum, log_entry->metadata.seqnum);
        return true;
    } else {
        return false;
    }
}

void LRUCache::PutAuxData(uint64_t seqnum, std::span<const char> data) {
    NOT_IMPLEMENTED();
}

bool GetAuxData(uint64_t seqnum, std::string* data) {
    NOT_IMPLEMENTED();
}

}  // namespace log
}  // namespace faas
