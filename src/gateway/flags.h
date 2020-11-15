#pragma once

#include <absl/flags/declare.h>
#include <absl/flags/flag.h>

#include "common/flags.h"

ABSL_DECLARE_FLAG(size_t, max_running_requests);
ABSL_DECLARE_FLAG(bool, lb_per_fn_round_robin);
ABSL_DECLARE_FLAG(bool, lb_pick_least_load);
