package statestore

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"

	gabs "github.com/Jeffail/gabs/v2"
	redis "github.com/go-redis/redis/v8"
)

var FLAGS_DisableAuxData bool = false
var FLAGS_RedisForAuxData bool = false

var redisClient *redis.Client

func init() {
	if val, exists := os.LookupEnv("DISABLE_AUXDATA"); exists && val == "1" {
		FLAGS_DisableAuxData = true
		log.Printf("[INFO] AuxData disabled")
	}
	if val, exists := os.LookupEnv("AUXDATA_REDIS_URL"); exists {
		FLAGS_RedisForAuxData = true
		log.Printf("[INFO] Use Redis for AuxData")
		opt, err := redis.ParseURL(val)
		if err != nil {
			log.Fatalf("[FATAL] Failed to parse Redis URL %s: %v", val, err)
		}
		redisClient = redis.NewClient(opt)
	}
}

const (
	LOG_NormalOp = iota
	LOG_TxnBegin
	LOG_TxnAbort
	LOG_TxnCommit
	LOG_NewObject
	LOG_DeleteObject
	LOG_Materialize
	LOG_GCWorker
)

type ObjectLogEntry struct {
	seqNum   uint64
	auxData  map[string]interface{}
	writeSet map[string]bool

	LogType int        `json:"t"`
	Ops     []*WriteOp `json:"o,omitempty"`
	TxnId   uint64     `json:"x"`

	ObjName string `json:"n,omitempty"`

	ObjData    interface{} `json:"d,omitempty"`
	NextSeqNum uint64      `json:"n"`

	GCShardId     int    `json:"s"`
	GCSafeTrimPos uint64 `json:"p"`
}

func objectLogTag(objNameHash uint64) uint64 {
	return (objNameHash << common.LogTagReserveBits) + common.ObjectLogTagLowBits
}

func txnHistoryLogTag(txnId uint64) uint64 {
	return (txnId << common.LogTagReserveBits) + common.TxnHistoryLogTagLowBits
}

func (env *envImpl) gcWorkerLogTag(objNameHash uint64) uint64 {
	if !env.gcEnabled {
		log.Fatal("[FATAL] GC is not enabled")
	}
	shard := objNameHash % uint64(env.gcNumShards)
	return (shard << common.LogTagReserveBits) + common.GCWorkerLogTagLowBits
}

func (l *ObjectLogEntry) fillWriteSet() {
	if l.LogType == LOG_NormalOp || l.LogType == LOG_TxnCommit {
		l.writeSet = make(map[string]bool)
		for _, op := range l.Ops {
			l.writeSet[op.ObjName] = true
		}
	}
}

func (l *ObjectLogEntry) encode() []byte {
	encoded, err := json.Marshal(l)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encoded)
}

func (l *ObjectLogEntry) decodeFrom(data []byte) {
	reader, err := common.DecompressReader(data)
	if err != nil {
		panic(err)
	}
	err = json.NewDecoder(reader).Decode(l)
	if err != nil {
		panic(err)
	}
}

func decodeLogEntry(logEntry *types.LogEntry) *ObjectLogEntry {
	objectLog := &ObjectLogEntry{}
	objectLog.decodeFrom(logEntry.Data)
	var auxData []byte
	if FLAGS_RedisForAuxData {
		key := fmt.Sprintf("%#016x", logEntry.SeqNum)
		val, err := redisClient.Get(context.Background(), key).Bytes()
		if err != nil {
			if err != redis.Nil {
				log.Fatalf("[FATAL] Failed to get AuxData from Redis: %v", err)
			}
		} else {
			auxData = val
		}
	} else {
		auxData = logEntry.AuxData
	}
	if len(auxData) > 0 {
		reader, err := common.DecompressReader(auxData)
		if err != nil {
			panic(err)
		}
		var contents map[string]interface{}
		err = json.NewDecoder(reader).Decode(&contents)
		if err != nil {
			panic(err)
		}
		objectLog.auxData = contents
	}
	objectLog.seqNum = logEntry.SeqNum
	objectLog.fillWriteSet()
	return objectLog
}

func (l *ObjectLogEntry) writeSetOverlapped(other *ObjectLogEntry) bool {
	if l.writeSet == nil || other.writeSet == nil {
		return false
	}
	for key, _ := range other.writeSet {
		if _, exists := l.writeSet[key]; exists {
			return true
		}
	}
	return false
}

func (l *ObjectLogEntry) withinWriteSet(objName string) bool {
	if l.writeSet == nil {
		return false
	}
	_, exists := l.writeSet[objName]
	return exists
}

func (txnCommitLog *ObjectLogEntry) checkTxnCommitResult(env *envImpl) (bool, error) {
	if txnCommitLog.LogType != LOG_TxnCommit {
		panic("Wrong log type")
	}
	if txnCommitLog.auxData != nil {
		if v, exists := txnCommitLog.auxData["r"]; exists {
			return v.(bool), nil
		}
	} else {
		txnCommitLog.auxData = make(map[string]interface{})
	}
	// log.Printf("[DEBUG] Failed to load txn status: seqNum=%#016x", txnCommitLog.seqNum)
	commitResult := true
	checkedTag := make(map[uint64]bool)
	for _, op := range txnCommitLog.Ops {
		tag := objectLogTag(common.NameHash(op.ObjName))
		if _, exists := checkedTag[tag]; exists {
			continue
		}
		seqNum := txnCommitLog.seqNum
		for seqNum > txnCommitLog.TxnId {
			objectLog, err := env.objectLogReadPrev(tag, seqNum-1)
			if err != nil {
				return false, newRuntimeError(err.Error())
			}
			if objectLog == nil || objectLog.seqNum <= txnCommitLog.TxnId {
				break
			}
			seqNum = objectLog.seqNum
			// log.Printf("[DEBUG] Read log with seqnum %#016x", seqNum)

			if !txnCommitLog.writeSetOverlapped(objectLog) {
				continue
			}
			if objectLog.LogType == LOG_NormalOp {
				commitResult = false
				break
			} else if objectLog.LogType == LOG_TxnCommit {
				if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
					return false, err
				} else if committed {
					commitResult = false
					break
				}
			}
		}
		if !commitResult {
			break
		}
		checkedTag[tag] = true
	}
	txnCommitLog.auxData["r"] = commitResult
	if !FLAGS_DisableAuxData {
		env.setLogAuxData(txnCommitLog.seqNum, txnCommitLog.auxData)
	}
	return commitResult, nil
}

func (l *ObjectLogEntry) hasCachedObjectView(objName string) bool {
	if l.auxData == nil {
		return false
	}
	if l.LogType == LOG_NormalOp {
		return true
	} else if l.LogType == LOG_TxnCommit {
		key := "v" + objName
		_, exists := l.auxData[key]
		return exists
	}
	return false
}

func (l *ObjectLogEntry) loadCachedObjectView(objName string) *ObjectView {
	if l.LogType == LOG_Materialize {
		if objName != l.ObjName {
			log.Fatalf("[FATAL] Object name mismatch: %s, %s", objName, l.ObjName)
		}
		return &ObjectView{
			name:       objName,
			nextSeqNum: l.NextSeqNum,
			contents:   gabs.Wrap(l.ObjData),
		}
	}
	if l.auxData == nil {
		return nil
	}
	if l.LogType == LOG_NormalOp {
		return &ObjectView{
			name:       objName,
			nextSeqNum: l.seqNum + 1,
			contents:   gabs.Wrap(l.auxData),
		}
	} else if l.LogType == LOG_TxnCommit {
		key := "v" + objName
		if data, exists := l.auxData[key]; exists {
			return &ObjectView{
				name:       objName,
				nextSeqNum: l.seqNum + 1,
				contents:   gabs.Wrap(data),
			}
		}
	}
	return nil
}

func (l *ObjectLogEntry) cacheObjectView(env *envImpl, view *ObjectView) {
	if FLAGS_DisableAuxData {
		return
	}
	if l.LogType == LOG_NormalOp {
		if l.auxData == nil {
			env.setLogAuxData(l.seqNum, view.contents.Data())
		}
	} else if l.LogType == LOG_TxnCommit {
		if l.auxData == nil {
			l.auxData = make(map[string]interface{})
		}
		key := "v" + view.name
		if _, exists := l.auxData[key]; !exists {
			l.auxData[key] = view.contents.Data()
			env.setLogAuxData(l.seqNum, l.auxData)
			delete(l.auxData, key)
		}
	} else {
		panic("Wrong log type")
	}
}

func (obj *ObjectRef) syncTo(tailSeqNum uint64) error {
	return obj.syncToBackwards(tailSeqNum)
}

/*
func (obj *ObjectRef) syncToForward(tailSeqNum uint64) error {
	tag := objectLogTag(obj.nameHash)
	env := obj.env
	if obj.view == nil {
		log.Fatalf("[FATAL] Empty object view: %s", obj.name)
	}
	seqNum := obj.view.nextSeqNum
	if tailSeqNum < seqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", seqNum, tailSeqNum)
	}
	for seqNum < tailSeqNum {
		logEntry, err := env.faasEnv.SharedLogReadNext(env.faasCtx, tag, seqNum)
		if err != nil {
			return newRuntimeError(err.Error())
		}
		if logEntry == nil || logEntry.SeqNum >= tailSeqNum {
			break
		}
		seqNum = logEntry.SeqNum + 1
		objectLog := decodeLogEntry(logEntry)
		if !objectLog.withinWriteSet(obj.name) {
			continue
		}
		if objectLog.LogType == LOG_TxnCommit {
			if committed, err := objectLog.checkTxnCommitResult(env); err != nil {
				return err
			} else if !committed {
				continue
			}
		}
		obj.view.nextSeqNum = objectLog.seqNum + 1
		for _, op := range objectLog.Ops {
			if op.ObjName == obj.name {
				obj.view.applyWriteOp(op)
			}
		}
		if !objectLog.hasCachedObjectView(obj.name) {
			objectLog.cacheObjectView(env, obj.view)
		}
	}
	return nil
}
*/

func (obj *ObjectRef) syncToBackwards(tailSeqNum uint64) error {
	if obj.view == nil {
		obj.view = newEmptyObjectView(obj.name)
	}
	if tailSeqNum < obj.view.nextSeqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", obj.view.nextSeqNum, tailSeqNum)
	}

	tag := objectLogTag(obj.nameHash)
	objectLogs := make([]*ObjectLogEntry, 0, 4)
	seqNum := tailSeqNum

	for seqNum > obj.view.nextSeqNum {
		if seqNum != protocol.MaxLogSeqnum {
			seqNum -= 1
		}
		objectLog, err := obj.env.objectLogReadPrev(tag, seqNum)
		if err != nil {
			return newRuntimeError(err.Error())
		}
		if objectLog == nil || objectLog.seqNum < obj.view.nextSeqNum {
			break
		}
		seqNum = objectLog.seqNum
		// log.Printf("[DEBUG] Read log with seqnum %#016x", seqNum)

		relevant := false
		switch objectLog.LogType {
		case LOG_NormalOp:
			relevant = objectLog.withinWriteSet(obj.name)
		case LOG_TxnCommit:
			if objectLog.withinWriteSet(obj.name) {
				committed, err := objectLog.checkTxnCommitResult(obj.env)
				if err != nil {
					return err
				}
				relevant = committed
			}
		case LOG_Materialize:
			if obj.name == objectLog.ObjName {
				relevant = true
			}
		default:
			log.Fatalf("[FATAL] Unknown log type for object %s: %d", obj.name, objectLog.LogType)
		}
		if !relevant {
			continue
		}

		if obj.isNew {
			obj.isNew = false
		}

		view := objectLog.loadCachedObjectView(obj.name)
		if view != nil && view.nextSeqNum > obj.view.nextSeqNum {
			// log.Printf("[DEBUG] Load cached view: seqNum=%#016x, obj=%s", seqNum, obj.name)
			obj.view = view
		}

		if objectLog.seqNum >= obj.view.nextSeqNum {
			objectLogs = append(objectLogs, objectLog)
		}
	}

	view := obj.view
	for i := len(objectLogs) - 1; i >= 0; i-- {
		objectLog := objectLogs[i]
		if objectLog.seqNum < view.nextSeqNum {
			log.Fatalf("[FATAL] LogSeqNum=%#016x, ViewNextSeqNum=%#016x", objectLog.seqNum, view.nextSeqNum)
		}
		view.nextSeqNum = objectLog.seqNum + 1
		for _, op := range objectLog.Ops {
			if op.ObjName == obj.name {
				view.applyWriteOp(op)
			}
		}
		objectLog.cacheObjectView(obj.env, view)
	}

	return nil
}

func (obj *ObjectRef) appendCreateLogIfNeeded() error {
	if !obj.env.gcEnabled {
		return nil
	}
	if err := obj.ensureView(); err != nil {
		return err
	}
	if obj.view.nextSeqNum > 0 {
		return nil
	}
	logEntry := &ObjectLogEntry{
		LogType: LOG_NewObject,
		ObjName: obj.name,
	}
	tags := []uint64{obj.env.gcWorkerLogTag(obj.nameHash)}
	_, err := obj.env.faasEnv.SharedLogAppend(obj.env.faasCtx, tags, logEntry.encode())
	if err != nil {
		return newRuntimeError(err.Error())
	}
	return nil
}

func (env *envImpl) appendDeleteLog(name string) error {
	if !env.gcEnabled {
		return nil
	}
	logEntry := &ObjectLogEntry{
		LogType: LOG_DeleteObject,
		ObjName: name,
	}
	tags := []uint64{env.gcWorkerLogTag(common.NameHash(name))}
	_, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, logEntry.encode())
	if err != nil {
		return newRuntimeError(err.Error())
	}
	return nil
}

func (obj *ObjectRef) clearNewBit() error {
	if obj.isNew {
		if err := obj.appendCreateLogIfNeeded(); err != nil {
			return err
		}
		obj.isNew = false
	}
	return nil
}

func (obj *ObjectRef) appendNormalOpLog(ops []*WriteOp) (uint64 /* seqNum */, error) {
	if len(ops) == 0 {
		panic("Empty Ops for NormalOp log")
	}
	if err := obj.clearNewBit(); err != nil {
		return 0, err
	}
	logEntry := &ObjectLogEntry{
		LogType: LOG_NormalOp,
		Ops:     ops,
	}
	tags := []uint64{objectLogTag(obj.nameHash)}
	seqNum, err := obj.env.faasEnv.SharedLogAppend(obj.env.faasCtx, tags, logEntry.encode())
	if err != nil {
		return 0, newRuntimeError(err.Error())
	} else {
		return seqNum, nil
	}
}

func (obj *ObjectRef) appendWriteLog(op *WriteOp) (uint64 /* seqNum */, error) {
	return obj.appendNormalOpLog([]*WriteOp{op})
}

func (env *envImpl) appendTxnBeginLog() (uint64 /* seqNum */, error) {
	logEntry := &ObjectLogEntry{LogType: LOG_TxnBegin}
	tags := []uint64{common.TxnMetaLogTag, common.GCMetaLogTag}
	seqNum, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, logEntry.encode())
	if err != nil {
		return 0, newRuntimeError(err.Error())
	} else {
		// log.Printf("[DEBUG] Append TxnBegin log: seqNum=%#016x", seqNum)
		return seqNum, nil
	}
}

func (env *envImpl) setLogAuxData(seqNum uint64, data interface{}) error {
	encoded, err := json.Marshal(data)
	if err != nil {
		panic(err)
	}
	compressed := common.CompressData(encoded)
	if FLAGS_RedisForAuxData {
		key := fmt.Sprintf("%#016x", seqNum)
		result := redisClient.Set(context.Background(), key, compressed, 0)
		if result.Err() != nil {
			log.Fatalf("[FATAL] Failed to set AuxData in Redis: %v", result.Err())
		}
		return nil
	}
	err = env.faasEnv.SharedLogSetAuxData(env.faasCtx, seqNum, compressed)
	if err != nil {
		return newRuntimeError(err.Error())
	} else {
		// log.Printf("[DEBUG] Set AuxData for log (seqNum=%#016x): contents=%s", seqNum, string(encoded))
		return nil
	}
}

func (view *ObjectView) materialize(env *envImpl) (bool, error) {
	if view.nextSeqNum == 0 {
		return false, nil
	}
	logEntry := &ObjectLogEntry{
		ObjName:    view.name,
		ObjData:    view.contents.Data(),
		NextSeqNum: view.nextSeqNum,
	}
	tags := []uint64{objectLogTag(common.NameHash(view.name))}
	_, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, logEntry.encode())
	if err != nil {
		return false, newRuntimeError(err.Error())
	}
	return true, nil
}

func (env *envImpl) appendGCWorkerLog(shardId int, safeTrimPos uint64) error {
	logEntry := &ObjectLogEntry{
		LogType:       LOG_GCWorker,
		GCShardId:     shardId,
		GCSafeTrimPos: safeTrimPos,
	}
	tags := []uint64{common.GCMetaLogTag}
	_, err := env.faasEnv.SharedLogAppend(env.faasCtx, tags, logEntry.encode())
	if err != nil {
		return newRuntimeError(err.Error())
	}
	return nil
}

func (env *envImpl) objectLogReadNext(tag uint64, minSeqNum uint64) (*ObjectLogEntry, error) {
	logEntry, err := env.faasEnv.SharedLogReadNext(env.faasCtx, tag, minSeqNum)
	if err != nil {
		return nil, err
	}
	if logEntry == nil {
		return nil, nil
	}
	return decodeLogEntry(logEntry), nil
}

func (env *envImpl) objectLogReadPrev(tag uint64, maxSeqNum uint64) (*ObjectLogEntry, error) {
	logEntry, err := env.faasEnv.SharedLogReadPrev(env.faasCtx, tag, maxSeqNum)
	if err != nil {
		return nil, err
	}
	if logEntry == nil {
		return nil, nil
	}
	return decodeLogEntry(logEntry), nil
}
