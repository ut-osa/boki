package statestore

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type Env interface {
	Object(name string) *ObjectRef
}

type envImpl struct {
	faasCtx context.Context
	faasEnv types.Environment
	txnCtx  *txnContext
}

func CreateEnv(ctx context.Context, faasEnv types.Environment) Env {
	return &envImpl{
		faasCtx: ctx,
		faasEnv: faasEnv,
		txnCtx:  nil,
	}
}
