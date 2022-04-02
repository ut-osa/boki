package statestore

import (
	"context"
	// "encoding/json"
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

type gcFuncHandler struct {
	env types.Environment
}

func NewGCFuncHandler(env types.Environment) *gcFuncHandler {
	return &gcFuncHandler{
		env: env,
	}
}

type objectState struct {
	name string
}

type gcState struct {
	gcLogPos    uint64
	prevTrimPos uint64
	objStates   map[string]*objectState
}

func newGCState() *gcState {
	return &gcState{
		gcLogPos:    0,
		prevTrimPos: 0,
		objStates:   make(map[string]*objectState),
	}
}

func (state *gcState) onNewObject(name string) {
	if _, exists := state.objStates[name]; !exists {
		state.objStates[name] = &objectState{
			name: name,
		}
		log.Printf("[INFO] GC see new object: %s", name)
	} else {
		log.Printf("[INFO] Redundant new object log: %s", name)
	}
}

func (state *gcState) readGCLogs(ctx context.Context, env types.Environment) error {
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
		if entry.LogType == LOG_NewObject {
			state.onNewObject(entry.ObjName)
		}
	}
	return nil
}

func (state *gcState) doTrim(ctx context.Context, env types.Environment) error {
	return nil
}

func (h *gcFuncHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	state := newGCState()
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
