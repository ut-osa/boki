#include "log/cache.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <absl/flags/flag.h>
#include <tkrzw_dbm_cache.h>
__END_THIRD_PARTY_HEADERS

ABSL_FLAG(bool, enforce_cache_miss_for_debug, false, "");

#define TKRZW_CHECK_OK(STATUS_VAR, OP_NAME)                 \
    do {                                                    \
        if (!(STATUS_VAR).IsOK()) {                         \
            LOG(FATAL) << "Tkrzw::" #OP_NAME " failed: "    \
                       << tkrzw::ToString(STATUS_VAR);      \
        }                                                   \
    } while (0)

namespace faas {
namespace log {

LRUCache::LRUCache(int mem_cap_mb) {
    int64_t cap_mem_size = -1;
    if (mem_cap_mb > 0) {
        cap_mem_size = int64_t{mem_cap_mb} << 20;
    }
    dbm_.reset(new tkrzw::CacheDBM(/* cap_rec_num= */ -1, cap_mem_size));
}

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

void LRUCache::PutBySeqnum(const LogMetaData& log_metadata,
                           std::span<const uint64_t> user_tags,
                           std::span<const char> log_data) {
    std::string data = EncodeLogEntry(log_metadata, user_tags, log_data);
    auto status = dbm_->Set(seqnum_key(log_metadata.seqnum), data,
                            /* overwrite= */ false);
    if (status == tkrzw::Status::DUPLICATION_ERROR) {
        return;
    }
    TKRZW_CHECK_OK(status, Set);
}

void LRUCache::PutBySeqnum(const LogEntry& log_entry) {
    PutBySeqnum(log_entry.metadata,
                VECTOR_AS_SPAN(log_entry.user_tags),
                STRING_AS_SPAN(log_entry.data));
}

std::optional<LogEntry> LRUCache::GetBySeqnum(uint64_t seqnum) {
    if (absl::GetFlag(FLAGS_enforce_cache_miss_for_debug)) {
        return std::nullopt;
    }
    std::string data;
    auto status = dbm_->Get(seqnum_key(seqnum), &data);
    if (status == tkrzw::Status::NOT_FOUND_ERROR) {
        return std::nullopt;
    }
    TKRZW_CHECK_OK(status, Get);
    LogEntry log_entry;
    DecodeLogEntry(std::move(data), &log_entry);
    DCHECK_EQ(seqnum, log_entry.metadata.seqnum);
    return log_entry;
}

void LRUCache::PutByLocalId(const LogMetaData& log_metadata,
                            std::span<const uint64_t> user_tags,
                            std::span<const char> log_data) {
    uint32_t logspace_id = bits::HighHalf64(log_metadata.seqnum);
    std::string data = EncodeLogEntry(log_metadata, user_tags, log_data);
    auto status = dbm_->Set(localid_key(logspace_id, log_metadata.localid), data,
                            /* overwrite= */ false);
    if (status == tkrzw::Status::DUPLICATION_ERROR) {
        return;
    }
    TKRZW_CHECK_OK(status, Set);
}

void LRUCache::PutByLocalId(const LogEntry& log_entry) {
    PutByLocalId(log_entry.metadata,
                 VECTOR_AS_SPAN(log_entry.user_tags),
                 STRING_AS_SPAN(log_entry.data));
}

std::optional<LogEntry> LRUCache::GetByLocalId(uint32_t logspace_id, uint64_t localid) {
    if (absl::GetFlag(FLAGS_enforce_cache_miss_for_debug)) {
        return std::nullopt;
    }
    std::string data;
    auto status = dbm_->Get(localid_key(logspace_id, localid), &data);
    if (status == tkrzw::Status::NOT_FOUND_ERROR) {
        return std::nullopt;
    }
    TKRZW_CHECK_OK(status, Get);
    LogEntry log_entry;
    DecodeLogEntry(std::move(data), &log_entry);
    DCHECK_EQ(localid, log_entry.metadata.localid);
    return log_entry;
}

void LRUCache::PutAuxData(uint64_t seqnum, std::span<const char> data) {
    dbm_->Set(aux_data_key(seqnum), std::string_view(data.data(), data.size()),
              /* overwrite= */ true);
}

std::optional<std::string> LRUCache::GetAuxData(uint64_t seqnum) {
    std::string data;
    auto status = dbm_->Get(aux_data_key(seqnum), &data);
    if (status == tkrzw::Status::NOT_FOUND_ERROR) {
        return std::nullopt;
    }
    TKRZW_CHECK_OK(status, Get);
    return data;
}

}  // namespace log
}  // namespace faas
