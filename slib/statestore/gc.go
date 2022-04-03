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

var FLAGS_GCSleepDuration time.Duration

func init() {
	if val, exists := os.LookupEnv("GC_SLEEP_DURATION_MS"); exists {
		durationMs, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		FLAGS_GCSleepDuration = time.Duration(durationMs) * time.Millisecond
	} else {
		FLAGS_GCSleepDuration = 100 * time.Millisecond
	}
	log.Printf("[INFO] GC sleep duration set to %s", FLAGS_GCSleepDuration)
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

func (h objectHeap) minSafeTrimPos() uint64 {
	if h.Len() == 0 {
		panic("Cannot call minSafeTrimPos on empty heap!")
	}
	return h[0].safeTrimPos
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
	env         *envImpl
	gcLogPos    uint64
	prevTrimPos uint64
	shardId     int
	objStates   map[string]*objectState
	objHeap     *objectHeap
}

func newGCWorkerState(env Env, input *GCFuncInput) *gcWorkerState {
	return &gcWorkerState{
		env:         env.(*envImpl),
		gcLogPos:    0,
		prevTrimPos: 0,
		shardId:     input.ShardId,
		objStates:   make(map[string]*objectState),
		objHeap:     &objectHeap{},
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

func (state *gcWorkerState) readGCLogs(ctx context.Context, env types.Environment) error {
	tag := (uint64(state.shardId) << common.LogTagReserveBits) + common.ObjectLogTagLowBits
	for {
		logEntry, err := env.SharedLogReadNext(ctx, tag, state.gcLogPos)
		if err != nil {
			return err
		}
		if logEntry == nil {
			return nil
		}
		entry := decodeLogEntry(logEntry)
		state.gcLogPos = logEntry.SeqNum + 1
		switch entry.LogType {
		case LOG_NewObject:
			state.onNewObject(entry.seqNum, entry.ObjName)
		case LOG_DeleteObject:
			state.onDeleteObject(entry.ObjName)
		default:
			return fmt.Errorf("Unknown log type: %d", entry.LogType)
		}
	}
	return nil
}

func (state *gcWorkerState) doTrim(ctx context.Context, env types.Environment) error {
	if len(state.objStates) == 0 {
		return nil
	}
	trimPos := state.objHeap.minSafeTrimPos()
	if trimPos <= state.prevTrimPos {
		return nil
	}
	log.Printf("[INFO] GC trim until seqnum %#016x", trimPos)
	state.prevTrimPos = trimPos
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
	state := newGCWorkerState(env, gcInput)
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

type gcControllerState struct {
	gcLogPos    uint64
	prevTrimPos uint64

	numShard         int
	shardSafeTrimPos []uint64
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
	return nil
}

func newGCControllerState(input *GCFuncInput) *gcControllerState {
	n := input.NumShard
	state := &gcControllerState{
		gcLogPos:         0,
		prevTrimPos:      0,
		numShard:         n,
		shardSafeTrimPos: make([]uint64, n),
	}
	for i := 0; i < n; i++ {
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
	state := newGCControllerState(gcInput)
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
