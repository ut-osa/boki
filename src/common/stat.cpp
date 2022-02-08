#include "common/stat.h"

#ifdef __FAAS_SRC

#include "common/flags.h"

__BEGIN_THIRD_PARTY_HEADERS
#include <absl/flags/reflection.h>
__END_THIRD_PARTY_HEADERS

namespace faas {
namespace stat {

bool StatEnabled(std::string_view statgroup_name) {
    if (absl::GetFlag(FLAGS_enable_all_stat)) {
        return true;
    }
    if (statgroup_name.empty()) {
        return false;
    }
    std::string flag_name = fmt::format("enable_statgroup_{}", statgroup_name);
    absl::CommandLineFlag* flag = absl::FindCommandLineFlag(flag_name);
    if (flag == nullptr) {
        LOG_F(FATAL, "Failed to find flag with name {}", flag_name);
    }
    if (auto enabled = flag->TryGet<bool>(); enabled.has_value()) {
        return enabled.value();
    } else {
        LOG_F(FATAL, "Failed to read bool flag name {}", flag_name);
    }
}

}  // namespace stat
}  // namespace faas

#endif  // defined(__FAAS_SRC)
