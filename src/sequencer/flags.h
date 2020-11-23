#pragma once

#include "common/flags.h"

ABSL_DECLARE_FLAG(int, raft_election_timeout_ms);
ABSL_DECLARE_FLAG(int, raft_heartbeat_timeout_ms);
ABSL_DECLARE_FLAG(int, raft_snapshot_threshold);
ABSL_DECLARE_FLAG(int, raft_snapshot_trailing);
ABSL_DECLARE_FLAG(bool, raft_pre_vote);
