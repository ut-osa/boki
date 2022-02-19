package sync

import (
	"context"
	"encoding/json"
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

const (
	GCLOG_NewQueue = iota
	GCLOG_Marker
)

type GCLogEntry struct {
	LogType   int    `json:"t"`
	QueueName string `json:"q"`

	// Marker type
	AuxData *QueueAuxData `json:"a,omitempty"`
}

func gcAppendLog(ctx context.Context, env types.Environment, entry *GCLogEntry) error {
	encoded, err := json.Marshal(entry)
	if err != nil {
		panic(err)
	}
	tags := []uint64{common.GCMetaLogTag}
	_, err = env.SharedLogAppend(ctx, tags, encoded)
	return err
}

func gcNewQueue(ctx context.Context, env types.Environment, name string) error {
	return gcAppendLog(ctx, env, &GCLogEntry{
		LogType:   GCLOG_NewQueue,
		QueueName: name,
	})
}

func gcNewMarker(ctx context.Context, env types.Environment, queueName string, auxData *QueueAuxData) error {
	return gcAppendLog(ctx, env, &GCLogEntry{
		LogType:   GCLOG_Marker,
		QueueName: queueName,
		AuxData:   auxData,
	})
}

type gcFuncHandler struct {
	env types.Environment
}

func NewGCFuncHandler(env types.Environment) *gcFuncHandler {
	return &gcFuncHandler{
		env: env,
	}
}

type gcState struct {
	gcLogPos      uint64
	prevTrimPos   uint64
	latestAuxData map[string]*QueueAuxData
}

func newGCState() *gcState {
	return &gcState{
		gcLogPos:      0,
		prevTrimPos:   0,
		latestAuxData: make(map[string]*QueueAuxData),
	}
}

func (state *gcState) onNewQueue(queueName string) {
	if _, exists := state.latestAuxData[queueName]; !exists {
		state.latestAuxData[queueName] = &QueueAuxData{
			NextSeqNum: 0,
			Consumed:   0,
			Tail:       0,
		}
		log.Printf("[INFO] GC see new queue: %s", queueName)
	}
}

func (state *gcState) onMarker(queueName string, auxData *QueueAuxData) {
	// log.Printf("[DEBUG] GC see new marker: queue=%s, consumed=%#016x", queueName, auxData.Consumed)
	if current, exists := state.latestAuxData[queueName]; exists {
		if auxData.NextSeqNum > current.NextSeqNum {
			state.latestAuxData[queueName] = auxData
		}
	} else {
		log.Fatalf("[FATAL] Failed to find queue: %s", queueName)
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
		entry := &GCLogEntry{}
		if err := json.Unmarshal(logEntry.Data, entry); err != nil {
			return err
		}
		state.gcLogPos = logEntry.SeqNum + 1
		if entry.LogType == GCLOG_NewQueue {
			state.onNewQueue(entry.QueueName)
		} else if entry.LogType == GCLOG_Marker {
			state.onMarker(entry.QueueName, entry.AuxData)
		}
	}
	return nil
}

func (state *gcState) doTrim(ctx context.Context, env types.Environment) error {
	trimPos := state.gcLogPos
	for _, auxData := range state.latestAuxData {
		if auxData.Consumed < trimPos {
			trimPos = auxData.Consumed
		}
	}
	if trimPos <= state.prevTrimPos {
		return nil
	}
	log.Printf("[INFO] GC trim until seqnum %#016x", trimPos)
	if err := env.SharedLogTrim(ctx, trimPos); err != nil {
		return err
	}
	state.prevTrimPos = trimPos
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
