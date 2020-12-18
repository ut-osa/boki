#pragma once

#include "common/flags.h"

ABSL_DECLARE_FLAG(bool, raft_enable_tracer);
ABSL_DECLARE_FLAG(int, raft_election_timeout_ms);
ABSL_DECLARE_FLAG(int, raft_heartbeat_timeout_ms);
ABSL_DECLARE_FLAG(bool, raft_enable_snapshot);
ABSL_DECLARE_FLAG(int, raft_snapshot_threshold);
ABSL_DECLARE_FLAG(int, raft_snapshot_trailing);
ABSL_DECLARE_FLAG(bool, raft_pre_vote);

ABSL_DECLARE_FLAG(bool, enable_raft_leader_fuzzer);
ABSL_DECLARE_FLAG(int, raft_leader_fuzz_interval_ms);

ABSL_DECLARE_FLAG(bool, enable_view_reconfig_fuzzer);
ABSL_DECLARE_FLAG(int, view_reconfig_fuzz_interval_ms);
