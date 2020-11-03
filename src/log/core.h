#pragma once

#include "log/common.h"
#include "log/view.h"
#include "log/storage.h"

namespace faas {
namespace log {

class Core {
public:
    explicit Core(uint16_t my_node_id);
    ~Core();

    void ApplySequencerMessage(std::span<const char> message);
    const View* GetView(uint16_t view_id) const;

    void NewLocalLog(uint32_t log_tag, std::span<const char> data);
    void NewRemoteLog(uint64_t log_localid, uint32_t log_tag, std::span<const char> data);

    bool ReadLog(uint64_t log_seqnum, std::span<const char>* data);

    std::string BuildLocalCutMessage();

private:
    uint16_t my_node_id_;

    View* current_view_;
    std::vector<std::unique_ptr<View>> views_;

    absl::flat_hash_map</* localid */ uint64_t, std::unique_ptr<LogEntry>> pending_entries_;
    absl::flat_hash_map</* node_id */ uint16_t, uint32_t> log_progress_;

    std::unique_ptr<Storage> storage_;

    DISALLOW_COPY_AND_ASSIGN(Core);
};

}  // namespace log
}  // namespace faas
