package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

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

func NewQueue(ctx context.Context, env types.Environment, name string) *Queue {
	return &Queue{
		ctx:        ctx,
		env:        env,
		name:       name,
		nameHash:   common.NameHash(name),
		consumed:   0,
		tail:       0,
		nextSeqNum: 0,
	}
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

func (q *Queue) appendPopLog() (uint64, error) {
	logEntry := &QueueLogEntry{
		QueueName: q.name,
		IsPush:    false,
	}
	encoded, err := json.Marshal(logEntry)
	if err != nil {
		panic(err)
	}
	tags := []uint64{queueLogTag(q.nameHash)}
	return q.env.SharedLogAppend(q.ctx, tags, encoded)
}

func (q *Queue) isEmpty() bool {
	return q.consumed >= q.tail
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

func (q *Queue) syncTo(tailSeqNum uint64) error {
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

var kQueueEmptyError = errors.New("Queue empty")
var kQueueTimeoutError = errors.New("Blocking pop timeout")

func IsQueueEmptyError(err error) bool {
	return err == kQueueEmptyError
}

func IsQueueTimeoutError(err error) bool {
	return err == kQueueTimeoutError
}

func (q *Queue) popNonblocking() (string /* payload */, error) {
	if err := q.syncTo(protocol.MaxLogSeqnum); err != nil {
		return "", err
	}
	if q.isEmpty() {
		return "", kQueueEmptyError
	}
	seqNum, err := q.appendPopLog()
	if err != nil {
		return "", err
	}
	if err := q.syncTo(seqNum); err != nil {
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

func (q *Queue) popBlocking() (string /* payload */, error) {
	return "", fmt.Errorf("Not implemented")
}

func (q *Queue) Pop(blocking bool) (string /* payload */, error) {
	if blocking {
		return q.popBlocking()
	} else {
		return q.popNonblocking()
	}
}
