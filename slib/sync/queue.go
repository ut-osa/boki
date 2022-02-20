package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

var FLAGS_GCEnabled bool = false
var FLAGS_GCMarkerDuration time.Duration = 1 * time.Second

func init() {
	if val, exists := os.LookupEnv("GC_ENABLED"); exists && val == "1" {
		FLAGS_GCEnabled = true
	}
	if val, exists := os.LookupEnv("GC_MARKER_DURATION_MS"); exists {
		durationMs, err := strconv.Atoi(val)
		if err != nil {
			panic(err)
		}
		FLAGS_GCMarkerDuration = time.Duration(durationMs) * time.Millisecond
	}
	if FLAGS_GCEnabled {
		log.Printf("[INFO] GC marker duration set to %s", FLAGS_GCMarkerDuration)
	}
}

type Queue struct {
	ctx context.Context
	env types.Environment

	name     string
	nameHash uint64

	consumed   uint64
	tail       uint64
	nextSeqNum uint64

	lastGCMarkerTime time.Time
	lastGCTrimPos    uint64
}

func queueLogTag(nameHash uint64) uint64 {
	return (nameHash << common.LogTagReserveBits) + common.QueueLogTagLowBits
}

func queuePushLogTag(nameHash uint64) uint64 {
	return (nameHash << common.LogTagReserveBits) + common.QueuePushLogTagLowBits
}

func NewQueue(ctx context.Context, env types.Environment, name string) (*Queue, error) {
	q := &Queue{
		ctx:              ctx,
		env:              env,
		name:             name,
		nameHash:         common.NameHash(name),
		consumed:         0,
		tail:             0,
		nextSeqNum:       0,
		lastGCMarkerTime: time.Unix(0, 0),
		lastGCTrimPos:    0,
	}
	if err := gcNewQueue(ctx, env, name); err != nil {
		return nil, err
	}
	if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Queue) Push(payload string) error {
	if len(payload) == 0 {
		return fmt.Errorf("Payload cannot be empty")
	}
	_, err := q.appendQueueLog(&QueueLogEntry{
		LogType: LOG_Push,
		Payload: payload,
	}, queueLogTag(q.nameHash), queuePushLogTag(q.nameHash))
	return err
}

func (q *Queue) isEmpty() bool {
	return q.consumed >= q.tail
}

func (q *Queue) findNextPush(minSeqNum, maxSeqNum uint64) (*QueueLogEntry, error) {
	tag := queuePushLogTag(q.nameHash)
	seqNum := minSeqNum
	for seqNum < maxSeqNum {
		logEntry, err := q.env.SharedLogReadNext(q.ctx, tag, seqNum)
		if err != nil {
			return nil, err
		}
		if logEntry == nil || logEntry.SeqNum >= maxSeqNum {
			return nil, nil
		}
		queueLog := &QueueLogEntry{}
		if err := queueLog.decodeFrom(logEntry); err != nil {
			panic(err)
		}
		if queueLog.isPush() && queueLog.QueueName == q.name {
			return queueLog, nil
		}
		seqNum = logEntry.SeqNum + 1
	}
	return nil, nil
}

func (q *Queue) applyLog(queueLog *QueueLogEntry) error {
	if queueLog.seqNum < q.nextSeqNum {
		log.Fatalf("[FATAL] LogSeqNum=%#016x, NextSeqNum=%#016x", queueLog.seqNum, q.nextSeqNum)
	}
	if queueLog.isPush() {
		q.tail = queueLog.seqNum + 1
	} else if queueLog.isPop() {
		nextLog, err := q.findNextPush(q.consumed, q.tail)
		if err != nil {
			return err
		}
		if nextLog != nil {
			q.consumed = nextLog.seqNum + 1
		} else {
			q.consumed = queueLog.seqNum
		}
	}
	q.nextSeqNum = queueLog.seqNum + 1
	return nil
}

func (q *Queue) gcMarker() error {
	if !FLAGS_GCEnabled {
		return nil
	}

	// log.Printf("[DEBUG] duration since last GC marker: %s", time.Since(q.lastGCMarkerTime))
	// log.Printf("[DEBUG] last GC trim position: %#016x", q.lastGCTrimPos)
	// log.Printf("[DEBUG] current consumed: %#016x", q.consumed)
	if time.Since(q.lastGCMarkerTime) <= FLAGS_GCMarkerDuration || q.consumed <= q.lastGCTrimPos {
		return nil
	}

	auxData := &QueueAuxData{
		NextSeqNum: q.nextSeqNum,
		Consumed:   q.consumed,
		Tail:       q.tail,
	}
	_, err := q.appendQueueLog(&QueueLogEntry{
		LogType: LOG_Aux,
		AuxData: auxData,
	}, queueLogTag(q.nameHash))
	if err != nil {
		return err
	}
	if err := gcNewMarker(q.ctx, q.env, q.name, auxData); err != nil {
		return err
	}
	q.lastGCMarkerTime = time.Now()
	q.lastGCTrimPos = q.consumed
	return nil
}

func (q *Queue) setAuxData(seqNum uint64, auxData *QueueAuxData) error {
	if auxData.NextSeqNum != seqNum+1 {
		return fmt.Errorf("Seqnum stored in auxdata not match with log entry")
	}
	encoded, err := json.Marshal(auxData)
	if err != nil {
		panic(err)
	}
	return q.env.SharedLogSetAuxData(q.ctx, seqNum, encoded)
}

func (q *Queue) syncTo(tailSeqNum uint64) error {
	if tailSeqNum < q.nextSeqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", q.nextSeqNum, tailSeqNum)
	}

	tag := queueLogTag(q.nameHash)
	queueLogs := make([]*QueueLogEntry, 0, 4)

	seqNumPos := tailSeqNum
	for seqNumPos > q.nextSeqNum {
		if seqNumPos != protocol.MaxLogSeqnum {
			seqNumPos -= 1
		}
		logEntry, err := q.env.SharedLogReadPrev(q.ctx, tag, seqNumPos)
		if err != nil {
			return err
		}
		if logEntry == nil || logEntry.SeqNum < q.nextSeqNum {
			break
		}
		seqNumPos = logEntry.SeqNum

		// Decode log entry into QueueLogEntry
		queueLog := &QueueLogEntry{}
		if err := queueLog.decodeFrom(logEntry); err != nil {
			panic(err)
		}
		if queueLog.QueueName != q.name {
			continue
		}
		q.updateGCInfoFromLogEntry(queueLog)

		// Check for auxdata
		if queueLog.isAuxBackup() || queueLog.hasAuxData() {
			auxData := queueLog.AuxData
			if auxData.NextSeqNum > q.nextSeqNum {
				q.nextSeqNum = auxData.NextSeqNum
				q.consumed = auxData.Consumed
				q.tail = auxData.Tail
			}
		}

		// Store log entries for apply
		if queueLog.seqNum >= q.nextSeqNum {
			if queueLog.isPush() || queueLog.isPop() {
				queueLogs = append(queueLogs, queueLog)
			}
		}
	}

	for i := len(queueLogs) - 1; i >= 0; i-- {
		queueLog := queueLogs[i]
		q.applyLog(queueLog)
		auxData := &QueueAuxData{
			NextSeqNum: q.nextSeqNum,
			Consumed:   q.consumed,
			Tail:       q.tail,
		}
		if err := q.setAuxData(queueLog.seqNum, auxData); err != nil {
			return err
		}
	}

	if err := q.gcMarker(); err != nil {
		return err
	}

	return nil
}

func (q *Queue) appendPopLogAndSync() error {
	seqNum, err := q.appendQueueLog(&QueueLogEntry{LogType: LOG_Pop}, queueLogTag(q.nameHash))
	if err != nil {
		return err
	}
	return q.syncTo(seqNum)
}

var kQueueEmptyError = errors.New("Queue empty")
var kQueueTimeoutError = errors.New("Blocking pop timeout")

func IsQueueEmptyError(err error) bool {
	return err == kQueueEmptyError
}

func IsQueueTimeoutError(err error) bool {
	return err == kQueueTimeoutError
}

func (q *Queue) Pop() (string /* payload */, error) {
	if q.isEmpty() {
		if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
			return "", err
		}
		if q.isEmpty() {
			return "", kQueueEmptyError
		}
	}
	if err := q.appendPopLogAndSync(); err != nil {
		return "", err
	}
	if nextLog, err := q.findNextPush(q.consumed, q.tail); err != nil {
		return "", err
	} else if nextLog != nil {
		return nextLog.Payload, nil
	} else {
		return "", kQueueEmptyError
	}
}

const kBlockingPopTimeout = 1 * time.Second

func (q *Queue) PopBlocking() (string /* payload */, error) {
	tag := queuePushLogTag(q.nameHash)
	startTime := time.Now()
	for time.Since(startTime) < kBlockingPopTimeout {
		if q.isEmpty() {
			if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
				return "", err
			}
		}
		if q.isEmpty() {
			seqNum := q.nextSeqNum
			for {
				// log.Printf("[DEBUG] BlockingRead: NextSeqNum=%#016x", seqNum)
				newCtx, _ := context.WithTimeout(q.ctx, kBlockingPopTimeout)
				logEntry, err := q.env.SharedLogReadNextBlock(newCtx, tag, seqNum)
				if err != nil {
					return "", err
				}
				if logEntry != nil {
					queueLog := &QueueLogEntry{}
					if err := queueLog.decodeFrom(logEntry); err != nil {
						panic(err)
					}
					if queueLog.isPush() && queueLog.QueueName == q.name {
						break
					}
					seqNum = logEntry.SeqNum + 1
				} else if time.Since(startTime) >= kBlockingPopTimeout {
					return "", kQueueTimeoutError
				}
			}
		}
		if err := q.appendPopLogAndSync(); err != nil {
			return "", err
		}
		if nextLog, err := q.findNextPush(q.consumed, q.tail); err != nil {
			return "", err
		} else if nextLog != nil {
			return nextLog.Payload, nil
		}
	}
	return "", kQueueTimeoutError
}
