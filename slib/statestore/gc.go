package statestore

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/types"
)

var FLAGS_GCSleepDuration time.Duration = 100 * time.Millisecond
var FLAGS_GCLogCountThreshold int = 32
var FLAGS_GCMinInterval time.Duration = 1 * time.Second
var FLAGS_GCMaxInterval time.Duration = 5 * time.Second

func init() {
	if val, exists := os.LookupEnv("GC_SLEEP_DURATION_MS"); exists {
		durationMs, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		FLAGS_GCSleepDuration = time.Duration(durationMs) * time.Millisecond
	}
	if val, exists := os.LookupEnv("GC_LOG_COUNT_THRESHOLD"); exists {
		threshold, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		FLAGS_GCLogCountThreshold = threshold
	}
	if val, exists := os.LookupEnv("GC_MIN_INTERVAL_MS"); exists {
		intervalMs, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		FLAGS_GCMinInterval = time.Duration(intervalMs) * time.Millisecond
	}
	if val, exists := os.LookupEnv("GC_MAX_INTERVAL_MS"); exists {
		intervalMs, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		FLAGS_GCMaxInterval = time.Duration(intervalMs) * time.Millisecond
	}

	log.Printf("[INFO] GC sleep duration set to %s", FLAGS_GCSleepDuration)
	log.Printf("[INFO] GC log count threshold set to %d", FLAGS_GCLogCountThreshold)
	log.Printf("[INFO] GC min interval set to %s", FLAGS_GCMinInterval)
	log.Printf("[INFO] GC max interval set to %s", FLAGS_GCMaxInterval)
}

type GCFuncInput struct {
	NumShard int `json:"numShard"`
	ShardId  int `json:"shardId"`
}

type objectState struct {
	name        string
	ref         *ObjectRef
	safeTrimPos uint64
	indexInHeap int
}

type objectHeap []*objectState

func (h objectHeap) Len() int {
	return len(h)
}

func (h objectHeap) Less(i, j int) bool {
	return h[i].safeTrimPos < h[j].safeTrimPos
}

func (h objectHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].indexInHeap = i
	h[j].indexInHeap = j
}

func (h objectHeap) minOne() *objectState {
	if h.Len() == 0 {
		panic("Cannot call minOne on empty heap!")
	}
	return h[0]
}

func (h *objectHeap) Push(x interface{}) {
	obj := x.(*objectState)
	obj.indexInHeap = h.Len()
	*h = append(*h, obj)
}

func (h *objectHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}

type gcWorkerState struct {
	env *envImpl

	gcLogPos uint64
	shardId  int

	objStates map[string]*objectState
	objHeap   *objectHeap

	prevTrimPos  uint64
	lastTrimTime time.Time
}

func newGCWorkerState(env Env, shardId int) *gcWorkerState {
	return &gcWorkerState{
		env:          env.(*envImpl),
		gcLogPos:     0,
		shardId:      shardId,
		objStates:    make(map[string]*objectState),
		objHeap:      &objectHeap{},
		prevTrimPos:  0,
		lastTrimTime: time.Now(),
	}
}

func (state *gcWorkerState) onNewObject(logPos uint64, name string) {
	if _, exists := state.objStates[name]; !exists {
		log.Printf("[INFO] GC see new object: %s", name)
		obj := &objectState{
			name:        name,
			ref:         state.env.Object(name),
			safeTrimPos: logPos,
			indexInHeap: -1,
		}
		state.objStates[name] = obj
		heap.Push(state.objHeap, obj)
	} else {
		log.Printf("[INFO] Redundant new object log: %s", name)
	}
}

func (state *gcWorkerState) onDeleteObject(name string) {
	if obj, exists := state.objStates[name]; exists {
		heap.Remove(state.objHeap, obj.indexInHeap)
		delete(state.objStates, name)
	} else {
		log.Printf("[WARN] Failed to find object with name %s", name)
	}
}

func (state *gcWorkerState) readGCLogs() error {
	env := state.env
	logEntry, err := env.faasEnv.SharedLogCheckTail(env.faasCtx, 0)
	if err != nil {
		return err
	}
	if logEntry == nil {
		return nil
	}
	tailSeqNum := logEntry.SeqNum
	if state.gcLogPos == tailSeqNum+1 {
		return nil
	}

	log.Printf("[DEBUG] Going to read until seqnum %#016x", tailSeqNum+1)

	tag := (uint64(state.shardId) << common.LogTagReserveBits) + common.GCWorkerLogTagLowBits
	for {
		entry, err := env.objectLogReadNext(tag, state.gcLogPos)
		if err != nil {
			return err
		}
		if entry == nil || entry.seqNum > tailSeqNum {
			break
		}
		state.gcLogPos = entry.seqNum + 1
		switch entry.LogType {
		case LOG_NewObject:
			state.onNewObject(entry.seqNum, entry.ObjName)
		case LOG_DeleteObject:
			state.onDeleteObject(entry.ObjName)
		default:
			return fmt.Errorf("Unknown log type: %d", entry.LogType)
		}
	}
	state.gcLogPos = tailSeqNum + 1
	return nil
}

func (state *gcWorkerState) doTrim() error {
	if state.gcLogPos == state.prevTrimPos {
		return nil
	}
	trimPos := state.gcLogPos

	for state.objHeap.Len() > 0 {
		obj := state.objHeap.minOne()
		trimPos = state.gcLogPos
		if pos := obj.safeTrimPos; pos < state.gcLogPos {
			trimPos = pos
		}
		if trimPos != state.prevTrimPos {
			break
		}

		if obj.ref.logCount < FLAGS_GCLogCountThreshold && time.Since(state.lastTrimTime) < FLAGS_GCMaxInterval {
			break
		}
		log.Printf("[DEBUG] Will materialize object %s: gcLogPos=%#016x, objSafeTrimPos=%#016x", obj.name, state.gcLogPos, obj.safeTrimPos)

		if err := obj.ref.syncTo(state.gcLogPos); err != nil {
			return err
		}
		view := obj.ref.view.Clone()
		view.nextSeqNum = state.gcLogPos
		if success, err := view.materialize(state.env); err != nil {
			return err
		} else if success {
			obj.safeTrimPos = state.gcLogPos
			heap.Fix(state.objHeap, obj.indexInHeap)
		} else {
			log.Printf("[WARN] Failed to materialize object %s", obj.name)
			return nil
		}
	}

	if trimPos < state.prevTrimPos {
		log.Fatalf("[FATAL] Trim position is before the previous: trimPos=%#016x, prevTrimPos=%#016x", trimPos, state.prevTrimPos)
	}
	if trimPos == state.prevTrimPos {
		return nil
	}
	if time.Since(state.lastTrimTime) <= FLAGS_GCMinInterval {
		return nil
	}
	log.Printf("[INFO] GC worker (shard %d) trims until seqnum %#016x", state.shardId, trimPos)
	if err := state.env.appendGCWorkerLog(state.shardId, trimPos); err != nil {
		return err
	}
	state.prevTrimPos = trimPos
	state.lastTrimTime = time.Now()
	return nil
}

type gcWorkerFuncHandler struct {
	env types.Environment
}

func NewGCWorkerFuncHandler(env types.Environment) *gcWorkerFuncHandler {
	return &gcWorkerFuncHandler{
		env: env,
	}
}

func (h *gcWorkerFuncHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	gcInput := &GCFuncInput{}
	if err := json.Unmarshal(input, gcInput); err != nil {
		log.Fatalf("[FATAL] Failed to decode input: %v", err)
	}
	env := CreateEnv(ctx, h.env)
	state := newGCWorkerState(env, gcInput.ShardId)
	log.Printf("[INFO] GC worker for shard %d starts", gcInput.ShardId)
	for {
		if err := state.readGCLogs(); err != nil {
			log.Fatalf("[FATAL] Failed to read GC logs: %v", err)
		}
		if err := state.doTrim(); err != nil {
			log.Fatalf("[FATAL] Failed to perform log trim: %v", err)
		}
		time.Sleep(FLAGS_GCSleepDuration)
	}
	return nil, nil
}

type gcControllerState struct {
	gcLogPos    uint64
	prevTrimPos uint64

	numShard         int
	shardSafeTrimPos []uint64
}

func (state *gcControllerState) onGCWorkerLog(shardId int, safeTrimPos uint64) {
	if shardId < 0 || shardId >= state.numShard {
		log.Fatalf("[FATAL] Invalid shard ID: %d", shardId)
	}
	if safeTrimPos > state.shardSafeTrimPos[shardId] {
		log.Printf("[INFO] Update safeTrimPos of shard %d to %#016x", shardId, safeTrimPos)
		state.shardSafeTrimPos[shardId] = safeTrimPos
	}
}

func (state *gcControllerState) onTxnBegin(txnId uint64) {
	panic("Not implemented")
}

func (state *gcControllerState) onTxnAbort(txnId uint64) {
	panic("Not implemented")
}

func (state *gcControllerState) onTxnCommit(txnId uint64) {
	panic("Not implemented")
}

func (state *gcControllerState) readGCLogs(ctx context.Context, env types.Environment) error {
	for {
		logEntry, err := env.SharedLogReadNext(ctx, common.GCMetaLogTag, state.gcLogPos)
		if err != nil {
			return err
		}
		if logEntry == nil {
			return nil
		}
		entry := decodeLogEntry(logEntry)
		state.gcLogPos = logEntry.SeqNum + 1
		switch entry.LogType {
		case LOG_GCWorker:
			state.onGCWorkerLog(entry.GCShardId, entry.GCSafeTrimPos)
		case LOG_TxnBegin:
			state.onTxnBegin(entry.seqNum)
		case LOG_TxnAbort:
			state.onTxnAbort(entry.TxnId)
		case LOG_TxnCommit:
			state.onTxnCommit(entry.TxnId)
		default:
			return fmt.Errorf("Unknown log type: %d", entry.LogType)
		}
	}
	return nil
}

func (state *gcControllerState) doTrim(ctx context.Context, env types.Environment) error {
	trimPos := state.gcLogPos
	for i := 0; i < state.numShard; i++ {
		if state.shardSafeTrimPos[i] < trimPos {
			trimPos = state.shardSafeTrimPos[i]
		}
	}
	if trimPos < state.prevTrimPos {
		log.Fatalf("[FATAL] Trim position is before the previous: trimPos=%#016x, prevTrimPos=%#016x", trimPos, state.prevTrimPos)
	}
	if trimPos == state.prevTrimPos {
		return nil
	}
	log.Printf("[INFO] GC trim until seqnum %#016x", trimPos)
	if err := env.SharedLogTrim(ctx, trimPos); err != nil {
		return err
	}
	state.prevTrimPos = trimPos
	return nil
}

func newGCControllerState(numShard int) *gcControllerState {
	state := &gcControllerState{
		gcLogPos:         0,
		prevTrimPos:      0,
		numShard:         numShard,
		shardSafeTrimPos: make([]uint64, numShard),
	}
	for i := 0; i < numShard; i++ {
		state.shardSafeTrimPos[i] = 0
	}
	return state
}

type gcControllerFuncHandler struct {
	env types.Environment
}

func NewGCControllerFuncHandler(env types.Environment) *gcControllerFuncHandler {
	return &gcControllerFuncHandler{
		env: env,
	}
}

func (h *gcControllerFuncHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	gcInput := &GCFuncInput{}
	if err := json.Unmarshal(input, gcInput); err != nil {
		log.Fatalf("[FATAL] Failed to decode input: %v", err)
	}
	state := newGCControllerState(gcInput.NumShard)
	log.Printf("[INFO] GC controller starts")
	for {
		if err := state.readGCLogs(ctx, h.env); err != nil {
			log.Fatalf("[FATAL] Failed to read GC logs: %v", err)
		}
		if err := state.doTrim(ctx, h.env); err != nil {
			log.Fatalf("[FATAL] Failed to perform log trim: %v", err)
		}
		time.Sleep(FLAGS_GCSleepDuration)
	}
	return nil, nil
}
