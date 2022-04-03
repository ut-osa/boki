package statestore

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/types"
)

var FLAGS_DisableTxn bool = false

func init() {
	if val, exists := os.LookupEnv("DISABLE_TXN"); exists && val == "1" {
		FLAGS_DisableTxn = true
		log.Printf("[INFO] Transaction disabled")
	}
}

type txnContext struct {
	active   bool
	readonly bool
	id       uint64 // TxnId is the seqnum of TxnBegin log
	ops      []*WriteOp
	writeSet []*ObjectRef
}

func CreateTxnEnv(ctx context.Context, faasEnv types.Environment) (Env, error) {
	if FLAGS_DisableTxn {
		return nil, newBadArgumentsError("transaction is globally disabled")
	}
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
	if FLAGS_DisableTxn {
		return nil, newBadArgumentsError("transaction is globally disabled")
	}
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
	tags := []uint64{common.TxnMetaLogTag, common.GCMetaLogTag, txnHistoryLogTag(ctx.id)}
	if _, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, common.CompressData(encoded)); err == nil {
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
	for _, o := range ctx.writeSet {
		if err := o.clearNewBit(); err != nil {
			return false, err
		}
	}
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
	tags := []uint64{common.TxnMetaLogTag, common.GCMetaLogTag, txnHistoryLogTag(ctx.id)}
	for _, op := range ctx.ops {
		tags = append(tags, objectLogTag(common.NameHash(op.ObjName)))
	}
	seqNum, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, common.CompressData(encoded))
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	// log.Printf("[DEBUG] Append TxnCommit log: seqNum=%#016x, op_size=%d", seqNum, len(ctx.ops))
	objectLog.fillWriteSet()
	objectLog.seqNum = seqNum
	// Check for status
	if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
		return false, err
	} else {
		return committed, nil
	}
}

func (ctx *txnContext) appendOp(obj *ObjectRef, op *WriteOp) {
	ctx.ops = append(ctx.ops, op)
	exists := false
	for _, o := range ctx.writeSet {
		if o.name == obj.name {
			exists = true
			break
		}
	}
	if !exists {
		ctx.writeSet = append(ctx.writeSet, obj)
	}
}
