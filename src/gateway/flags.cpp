#include "gateway/flags.h"

ABSL_FLAG(size_t, max_running_requests, 0, "");
ABSL_FLAG(bool, lb_per_fn_round_robin, false, "");
ABSL_FLAG(bool, lb_pick_least_load, false, "");
