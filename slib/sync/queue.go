package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

type Queue struct {
	ctx context.Context
	env types.Environment

	name     string
	nameHash uint64

	consumed   uint64
	tail       uint64
	nextSeqNum uint64

	nonce int
}

type QueueAuxData struct {
	Consumed uint64 `json:"h"`
	Tail     uint64 `json:"t"`
}

type QueueLogEntry struct {
	seqNum  uint64
	auxData *QueueAuxData

	QueueName string `json:"n"`
	IsPush    bool   `json:"t"`
	Payload   string `json:"p,omitempty"`
}

func queueLogTag(nameHash uint64) uint64 {
	return (nameHash << common.LogTagReserveBits) + common.QueueLogTagLowBits
}

func queuePushLogTag(nameHash uint64) uint64 {
	return (nameHash << common.LogTagReserveBits) + common.QueuePushLogTagLowBits
}

func decodeQueueLogEntry(logEntry *types.LogEntry) *QueueLogEntry {
	queueLog := &QueueLogEntry{}
	err := json.Unmarshal(logEntry.Data, queueLog)
	if err != nil {
		panic(err)
	}
	if len(logEntry.AuxData) > 0 {
		auxData := &QueueAuxData{}
		err := json.Unmarshal(logEntry.AuxData, auxData)
		if err != nil {
			panic(err)
		}
		queueLog.auxData = auxData
	}
	queueLog.seqNum = logEntry.SeqNum
	return queueLog
}

func NewQueue(ctx context.Context, env types.Environment, name string) (*Queue, error) {
	q := &Queue{
		ctx:        ctx,
		env:        env,
		name:       name,
		nameHash:   common.NameHash(name),
		consumed:   0,
		tail:       0,
		nextSeqNum: 0,
		nonce:      0,
	}
	if err := q.syncToBackward(protocol.MaxLogSeqnum); err != nil {
		return nil, err
	}
	return q, nil
}

func (q *Queue) Push(payload string) error {
	if len(payload) == 0 {
		return fmt.Errorf("Payload cannot be empty")
	}
	logEntry := &QueueLogEntry{
		QueueName: q.name,
		IsPush:    true,
		Payload:   payload,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []uint64{queueLogTag(q.nameHash), queuePushLogTag(q.nameHash)}
	_, err = q.env.SharedLogAppend(q.ctx, tags, encoded)
	return err
}

func (q *Queue) isEmpty() bool {
	return q.consumed >= q.tail
}

func (q *Queue) coin() bool {
	q.nonce++
	return q.nonce%2 == 0
}

func (q *Queue) findNext(minSeqNum, maxSeqNum uint64) (*QueueLogEntry, error) {
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
		queueLog := decodeQueueLogEntry(logEntry)
		if queueLog.IsPush && queueLog.QueueName == q.name {
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
	if queueLog.IsPush {
		q.tail = queueLog.seqNum + 1
	} else {
		nextLog, err := q.findNext(q.consumed, q.tail)
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

func (q *Queue) setAuxData(seqNum uint64, auxData *QueueAuxData) error {
	encoded, err := json.Marshal(auxData)
	if err != nil {
		panic(err)
	}
	return q.env.SharedLogSetAuxData(q.ctx, seqNum, encoded)
}

func (q *Queue) syncToBackward(tailSeqNum uint64) error {
	if tailSeqNum < q.nextSeqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", q.nextSeqNum, tailSeqNum)
	}
	if tailSeqNum == q.nextSeqNum {
		return nil
	}

	tag := queueLogTag(q.nameHash)
	queueLogs := make([]*QueueLogEntry, 0, 4)

	seqNum := tailSeqNum
	for seqNum > q.nextSeqNum {
		if seqNum != protocol.MaxLogSeqnum {
			seqNum -= 1
		}
		logEntry, err := q.env.SharedLogReadPrev(q.ctx, tag, seqNum)
		if err != nil {
			return err
		}
		if logEntry == nil || logEntry.SeqNum < q.nextSeqNum {
			break
		}
		seqNum = logEntry.SeqNum
		queueLog := decodeQueueLogEntry(logEntry)
		if queueLog.QueueName != q.name {
			continue
		}
		if queueLog.auxData != nil {
			q.nextSeqNum = queueLog.seqNum + 1
			q.consumed = queueLog.auxData.Consumed
			q.tail = queueLog.auxData.Tail
			break
		} else {
			queueLogs = append(queueLogs, queueLog)
		}
	}

	for i := len(queueLogs) - 1; i >= 0; i-- {
		queueLog := queueLogs[i]
		q.applyLog(queueLog)
		auxData := &QueueAuxData{
			Consumed: q.consumed,
			Tail:     q.tail,
		}
		if err := q.setAuxData(queueLog.seqNum, auxData); err != nil {
			return err
		}
	}
	return nil
}

func (q *Queue) syncToForward(tailSeqNum uint64) error {
	if tailSeqNum < q.nextSeqNum {
		log.Fatalf("[FATAL] Current seqNum=%#016x, cannot sync to %#016x", q.nextSeqNum, tailSeqNum)
	}
	tag := queueLogTag(q.nameHash)
	seqNum := q.nextSeqNum
	for seqNum < tailSeqNum {
		logEntry, err := q.env.SharedLogReadNext(q.ctx, tag, seqNum)
		if err != nil {
			return err
		}
		if logEntry == nil || logEntry.SeqNum >= tailSeqNum {
			break
		}
		seqNum = logEntry.SeqNum + 1
		queueLog := decodeQueueLogEntry(logEntry)
		if queueLog.QueueName == q.name {
			q.applyLog(queueLog)
			if queueLog.auxData == nil {
				auxData := &QueueAuxData{
					Consumed: q.consumed,
					Tail:     q.tail,
				}
				if err := q.setAuxData(queueLog.seqNum, auxData); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (q *Queue) syncTo(tailSeqNum uint64) error {
	return q.syncToBackward(tailSeqNum)
}

func (q *Queue) appendPopLogAndSync() error {
	logEntry := &QueueLogEntry{
		QueueName: q.name,
		IsPush:    false,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []uint64{queueLogTag(q.nameHash)}
	if seqNum, err := q.env.SharedLogAppend(q.ctx, tags, encoded); err != nil {
		return err
	} else {
		return q.syncTo(seqNum)
	}
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
	if nextLog, err := q.findNext(q.consumed, q.tail); err != nil {
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
					queueLog := decodeQueueLogEntry(logEntry)
					if queueLog.IsPush && queueLog.QueueName == q.name {
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
		if nextLog, err := q.findNext(q.consumed, q.tail); err != nil {
			return "", err
		} else if nextLog != nil {
			return nextLog.Payload, nil
		}
	}
	return "", kQueueTimeoutError
}
