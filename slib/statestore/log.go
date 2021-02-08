package statestore

import (
	"encoding/json"
	"log"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
	gabs "github.com/Jeffail/gabs/v2"
)

const (
	LOG_NormalOp = iota
	LOG_TxnBegin
	LOG_TxnAbort
	LOG_TxnCommit
)

type ObjectLogEntry struct {
	SeqNum  uint64     `json:"-"`
	LogType int        `json:"t"`
	ObjName string     `json:"n,omitempty"`
	Ops     []*WriteOp `json:"o,omitempty"`
}

const kLogTagReserveBits = 3
const kObjectLogTagLowBits = 1

func objectLogTag(objNameHash uint64) uint64 {
	return (objNameHash << kLogTagReserveBits) + kObjectLogTagLowBits
}

func createObjectView(logEntry *types.LogEntry) *ObjectView {
	if len(logEntry.AuxData) == 0 {
		return nil
	}
	var contents map[string]interface{}
	err := json.Unmarshal(logEntry.AuxData, &contents)
	if err != nil {
		panic(err)
	}
	return &ObjectView{
		nextSeqNum: logEntry.SeqNum + 1,
		contents:   gabs.Wrap(contents),
	}
}

func setObjectViewCache(env *envImpl, seqNum uint64, view *ObjectView) error {
	err := env.faasEnv.SharedLogSetAuxData(env.faasCtx, seqNum, view.contents.Bytes())
	if err != nil {
		return newRuntimeError(err.Error())
	} else {
		return nil
	}
}

func (obj *ObjectRef) SyncTo(tailSeqNum uint64) error {
	tag := objectLogTag(obj.nameHash)
	env := obj.env
	objectLogs := make([]*ObjectLogEntry, 0)
	var view *ObjectView
	seqNum := tailSeqNum
	currentSeqNum := uint64(0)
	if obj.view != nil {
		currentSeqNum = obj.view.nextSeqNum
	}

	for seqNum > currentSeqNum {
		if seqNum != protocol.MaxLogSeqnum {
			seqNum -= 1
		}
		logEntry, err := env.faasEnv.SharedLogReadPrev(env.faasCtx, tag, seqNum)
		if err != nil {
			return newRuntimeError(err.Error())
		}
		if logEntry == nil {
			break
		}
		// log.Printf("[DEBUG] Read log with seqnum %#016x", logEntry.SeqNum)
		var objectLog ObjectLogEntry
		err = json.Unmarshal(logEntry.Data, &objectLog)
		if err != nil {
			return err
		}
		switch objectLog.LogType {
		case LOG_NormalOp:
			if objectLog.ObjName == obj.name {
				view = createObjectView(logEntry)
				if view == nil {
					objectLog.SeqNum = logEntry.SeqNum
					objectLogs = append(objectLogs, &objectLog)
				}
			}
		default:
			panic("Not implemented")
		}
		if view != nil {
			break
		}
		seqNum = logEntry.SeqNum
	}

	if view == nil {
		if obj.view != nil {
			view = obj.view
		} else {
			view = &ObjectView{
				nextSeqNum: 0,
				contents:   gabs.New(),
			}
		}
	}
	for i := len(objectLogs) - 1; i >= 0; i-- {
		objectLog := objectLogs[i]
		if objectLog.SeqNum < view.nextSeqNum {
			log.Fatalf("[FATAL] LogSeqNum=%#016x, ViewNextSeqNum=%#016x", objectLog.SeqNum, view.nextSeqNum)
		}
		switch objectLog.LogType {
		case LOG_NormalOp:
			view.applyNormalOpLog(objectLog.SeqNum, objectLog.Ops)
			if err := setObjectViewCache(env, objectLog.SeqNum, view); err != nil {
				return err
			}
		default:
			panic("Not implemented")
		}
	}
	obj.view = view
	return nil
}

func (obj *ObjectRef) Sync() error {
	return obj.SyncTo(protocol.MaxLogSeqnum)
}

func (obj *ObjectRef) appendNormalOpLog(ops []*WriteOp) (uint64 /* seqNum */, error) {
	logEntry := ObjectLogEntry{
		SeqNum:  0,
		LogType: LOG_NormalOp,
		ObjName: obj.name,
		Ops:     ops,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tag := objectLogTag(obj.nameHash)
	seqNum, err := obj.env.faasEnv.SharedLogAppend(obj.env.faasCtx, []uint64{tag}, encoded)
	if err != nil {
		return 0, newRuntimeError(err.Error())
	} else {
		return seqNum, nil
	}
}

func (obj *ObjectRef) appendWriteLog(op *WriteOp) (uint64 /* seqNum */, error) {
	return obj.appendNormalOpLog([]*WriteOp{op})
}

func (view *ObjectView) applyNormalOpLog(seqNum uint64, ops []*WriteOp) {
	view.nextSeqNum = seqNum + 1
	for _, op := range ops {
		view.applyWriteOp(op)
	}
}
