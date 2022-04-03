package statestore

import (
	"context"
	"log"
	"os"
	"strconv"

	"cs.utexas.edu/zjia/faas/types"
)

var FLAGS_GCNumShards int = -1

func init() {
	if val, exists := os.LookupEnv("GC_NUM_SHARDS"); exists {
		n, err := strconv.Atoi(val)
		if err != nil {
			log.Fatalf("[FATAL] Failed to parse GC_NUM_SHARDS: %v", err)
		}
		if n <= 0 {
			log.Fatalf("[FATAL] Invalid number of shards: %d", n)
		}
		FLAGS_GCNumShards = n
	}
}

type Env interface {
	Object(name string) *ObjectRef
	TxnCommit() (bool /* committed */, error)
	TxnAbort() error
}

type envImpl struct {
	faasCtx     context.Context
	faasEnv     types.Environment
	objs        map[string]*ObjectRef
	txnCtx      *txnContext
	gcEnabled   bool
	gcNumShards int
}

func CreateEnv(ctx context.Context, faasEnv types.Environment) Env {
	return &envImpl{
		faasCtx:     ctx,
		faasEnv:     faasEnv,
		objs:        make(map[string]*ObjectRef),
		txnCtx:      nil,
		gcEnabled:   FLAGS_GCNumShards != -1,
		gcNumShards: FLAGS_GCNumShards,
	}
}
