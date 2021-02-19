package statestore

import (
	"context"
	"encoding/json"

	"cs.utexas.edu/zjia/faas/types"
)

type txnContext struct {
	active   bool
	readonly bool
	id       uint64 // TxnId is the seqnum of TxnBegin log
	ops      []*WriteOp
}

func CreateTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	if seqNum, err := env.appendTxnBeginLog(); err == nil {
		env.txnCtx = &txnContext{
			active:   true,
			readonly: false,
			id:       seqNum,
			ops:      make([]*WriteOp, 0, 4),
		}
		return env, nil
	} else {
		return nil, err
	}
}

func CreateReadOnlyTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	env := CreateEnv(ctx, faasEnv).(*envImpl)
	if tail, err := faasEnv.SharedLogCheckTail(ctx, 0 /* tag */); err == nil {
		seqNum := uint64(0)
		if tail != nil {
			seqNum = tail.SeqNum + 1
		}
		env.txnCtx = &txnContext{
			active:   true,
			readonly: true,
			id:       seqNum,
			ops:      nil,
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
	if env.txnCtx.readonly {
		panic("Read-only transaction")
	}
	ctx := env.txnCtx
	env.txnCtx = nil
	ctx.active = false
	logEntry := ObjectLogEntry{
		LogType: LOG_TxnAbort,
		TxnId:   ctx.id,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []uint64{kTxnMetaLogTag, txnHistoryLogTag(ctx.id)}
	if _, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, compressData(encoded)); err == nil {
		return nil
	} else {
		return newRuntimeError(err.Error())
	}
}

func (env *envImpl) TxnCommit() (bool /* committed */, error) {
	if env.txnCtx == nil {
		panic("Not in a transaction env")
	}
	if env.txnCtx.readonly {
		panic("Read-only transaction")
	}
	ctx := env.txnCtx
	env.txnCtx = nil
	ctx.active = false
	// Append commit log
	objectLog := &ObjectLogEntry{
		LogType: LOG_TxnCommit,
		Ops:     ctx.ops,
		TxnId:   ctx.id,
	}
	encoded, err := json.Marshal(objectLog)
	if err != nil {
		panic(err)
	}
	tags := []uint64{kTxnMetaLogTag, txnHistoryLogTag(ctx.id)}
	for _, op := range ctx.ops {
		tags = append(tags, objectLogTag(objectNameHash(op.ObjName)))
	}
	seqNum, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, compressData(encoded))
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	// log.Printf("[DEBUG] Append TxnCommit log: seqNum=%#016x, op_size=%d", seqNum, len(ctx.ops))
	// Read back the commit log
	logEntry, err := env.faasEnv.SharedLogReadNext(env.faasCtx, 0, seqNum)
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	if logEntry == nil || logEntry.SeqNum != seqNum {
		panic("Cannot read the log just appended")
	}
	objectLog = decodeLogEntry(logEntry)
	// Check for status
	if committed, err := objectLog.checkTxnCommitResult(env); err == nil {
		return committed, nil
	} else {
		return false, err
	}
}

func (ctx *txnContext) appendOp(op *WriteOp) {
	ctx.ops = append(ctx.ops, op)
}
