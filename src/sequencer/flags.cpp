#include "sequencer/flags.h"

ABSL_FLAG(int, raft_election_timeout_ms, 5, "");
ABSL_FLAG(int, raft_heartbeat_timeout_ms, 2, "");
ABSL_FLAG(int, raft_snapshot_threshold, 1024, "");
ABSL_FLAG(int, raft_snapshot_trailing, 128, "");
ABSL_FLAG(bool, raft_pre_vote, false, "");
