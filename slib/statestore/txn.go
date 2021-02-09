package statestore

import (
	"context"

	"cs.utexas.edu/zjia/faas/types"
)

type txnContext struct {
	id      uint64 // TxnId is the seqnum of TxnBegin log
	ops     []*WriteOp
	results []*WriteResult
}

func CreateTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	if seqNum, err := env.appendTxnBeginLog(); err == nil {
		env.txnCtx = &txnContext{
			id:      seqNum,
			ops:     make([]*WriteOp, 0, 4),
			results: make([]*WriteResult, 0, 4),
		}
		return env, nil
	} else {
		return nil, err
	}
}

func (env *envImpl) TxnAbort() error {
	if env.txnCtx == nil {
		panic("Not in a transaction env")
	}
	return nil
}

func (env *envImpl) TxnCommit() (bool /* committed */, error) {
	if env.txnCtx == nil {
		panic("Not in a transaction env")
	}
	return false, nil
}

func (ctx *txnContext) appendOp(op *WriteOp, result *WriteResult) {
	ctx.ops = append(ctx.ops, op)
	ctx.results = append(ctx.results, result)
}
