package sync

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type QueueAuxData struct {
	NextSeqNum uint64 `json:"n"`
	Consumed   uint64 `json:"h"`
	Tail       uint64 `json:"t"`
}

const (
	LOG_Push = iota
	LOG_Pop
	LOG_Aux
)

type QueueLogEntry struct {
	seqNum uint64
	hasAux bool

	LogType   int    `json:"t"`
	QueueName string `json:"n"`

	// Push type
	Payload string `json:"p,omitempty"`

	// Aux type
	AuxData *QueueAuxData `json:"a,omitempty"`

	// For GC
	GCPayload1 string `json:"gc1,omitempty"`
	GCPayload2 string `json:"gc2,omitempty"`
}

func (entry *QueueLogEntry) isPush() bool {
	return entry.LogType == LOG_Push
}

func (entry *QueueLogEntry) isPop() bool {
	return entry.LogType == LOG_Pop
}

func (entry *QueueLogEntry) isAuxBackup() bool {
	return entry.LogType == LOG_Aux
}

func (entry *QueueLogEntry) hasAuxData() bool {
	if entry.isPush() || entry.isPop() {
		return entry.AuxData != nil
	}
	return false
}

func (entry *QueueLogEntry) decodeFrom(logEntry *types.LogEntry) error {
	entry.seqNum = logEntry.SeqNum
	entry.hasAux = false
	err := json.Unmarshal(logEntry.Data, entry)
	if err != nil {
		return err
	}
	if entry.LogType != LOG_Aux && len(logEntry.AuxData) > 0 {
		auxData := &QueueAuxData{}
		err := json.Unmarshal(logEntry.AuxData, auxData)
		if err != nil {
			return err
		}
		if auxData.NextSeqNum != entry.seqNum+1 {
			return fmt.Errorf("Seqnum stored in auxdata not match with log entry")
		}
		entry.hasAux = true
		entry.AuxData = auxData
	}
	return nil
}

func (q *Queue) appendQueueLog(entry *QueueLogEntry, tags ...uint64) (uint64, error) {
	if entry.QueueName == "" {
		entry.QueueName = q.name
	} else if entry.QueueName != q.name {
		log.Fatalf("[FATAL] Queue name in log entry is %s, expect %s", entry.QueueName, q.name)
	}
	entry.GCPayload1 = fmt.Sprintf("%016x", q.lastGCMarkerTime.UnixNano())
	entry.GCPayload2 = fmt.Sprintf("%016x", q.lastGCTrimPos)
	encoded, err := json.Marshal(entry)
	if err != nil {
		panic(err)
	}
	return q.env.SharedLogAppend(q.ctx, tags, encoded)
}

func (q *Queue) updateGCInfoFromLogEntry(entry *QueueLogEntry) {
	if s, err := strconv.ParseInt(entry.GCPayload1, 16, 64); err == nil {
		if s > q.lastGCMarkerTime.UnixNano() {
			q.lastGCMarkerTime = time.Unix(0, s)
		}
	}
	if s, err := strconv.ParseUint(entry.GCPayload2, 16, 64); err == nil {
		if s > q.lastGCTrimPos {
			q.lastGCTrimPos = s
		}
	}
}
