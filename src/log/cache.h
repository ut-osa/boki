#pragma once

#include "log/common.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <tkrzw_dbm_cache.h>
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace log {

class LRUCache {
public:
    explicit LRUCache(int mem_cap_mb);
    ~LRUCache() = default;

    void PutBySeqnum(const LogMetaData& log_metadata,
                     std::span<const uint64_t> user_tags,
                     std::span<const char> log_data);
    void PutBySeqnum(const LogEntry& log_entry);
    std::optional<LogEntry> GetBySeqnum(uint64_t seqnum);

    void PutByLocalId(const LogMetaData& log_metadata,
                      std::span<const uint64_t> user_tags,
                      std::span<const char> log_data);
    void PutByLocalId(const LogEntry& log_entry);
    std::optional<LogEntry> GetByLocalId(uint32_t logspace_id, uint64_t localid);

    void PutAuxData(uint64_t seqnum, std::span<const char> data);
    std::optional<std::string> GetAuxData(uint64_t seqnum);

private:
    std::unique_ptr<tkrzw::CacheDBM> dbm_;

    void GetLogEntryByKey(std::string_view key);

    inline std::string seqnum_key(uint64_t seqnum) {
        return fmt::format("0_{:016x}", seqnum);
    }

    inline std::string localid_key(uint32_t logspace_id, uint64_t localid) {
        DCHECK((localid >> 48) == 0);
        return fmt::format("1_{:08x}{:012x}", logspace_id, localid);
    }

    inline std::string aux_data_key(uint64_t seqnum) {
        return fmt::format("2_{:016x}", seqnum);
    }

    DISALLOW_COPY_AND_ASSIGN(LRUCache);
};

}  // namespace log
}  // namespace faas
