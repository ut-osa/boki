package worker

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	protocol "cs.utexas.edu/zjia/faas/protocol"
	types "cs.utexas.edu/zjia/faas/types"
)

func (w *FuncWorker) allocNextLogOp() (uint64 /* opId */, uint64 /* currentCallId */) {
	opId := atomic.AddUint64(&w.nextLogOpId, 1)
	currentCallId := atomic.LoadUint64(&w.currentCall)
	return opId, currentCallId
}

func (w *FuncWorker) sendSharedLogMessage(opId uint64, message []byte) (chan []byte /* respChan */, error) {
	respChan := make(chan []byte, 1)
	w.mux.Lock()
	w.outgoingLogOps[opId] = respChan
	_, err := w.outputPipe.Write(message)
	w.mux.Unlock()
	if err != nil {
		return nil, err
	} else {
		return respChan, nil
	}
}

func checkAndDuplicateTags(tags []uint64) ([]uint64, error) {
	if len(tags) == 0 {
		return nil, nil
	}
	tagSet := make(map[uint64]bool)
	for _, tag := range tags {
		if tag == 0 || ^tag == 0 {
			return nil, fmt.Errorf("Invalid tag: %v", tag)
		}
		tagSet[tag] = true
	}
	results := make([]uint64, 0, len(tags))
	for tag, _ := range tagSet {
		results = append(results, tag)
	}
	return results, nil
}

// Implement types.Environment
func (w *FuncWorker) SharedLogAppend(ctx context.Context, tags []uint64, data []byte) (uint64, error) {
	if len(data) == 0 {
		return 0, fmt.Errorf("Data cannot be empty")
	}
	tags, err := checkAndDuplicateTags(tags)
	if err != nil {
		return 0, err
	}

	sleepDuration := 5 * time.Millisecond
	remainingRetries := 4

	for {
		id, currentCallId := w.allocNextLogOp()
		message := protocol.NewSharedLogAppendMessage(currentCallId, w.clientId, uint16(len(tags)), id)

		var encodedData []byte
		if len(tags) == 0 {
			encodedData = data
		} else {
			tagBuffer := protocol.BuildLogTagsBuffer(tags)
			encodedData = bytes.Join([][]byte{tagBuffer, data}, nil /* sep */)
		}

		if len(encodedData) <= protocol.MessageInlineDataSize {
			protocol.FillInlineDataInMessage(message, encodedData)
		} else {
			auxBuf := &AuxBuffer{
				id:   w.GenerateUniqueID(),
				data: encodedData,
			}
			// log.Printf("[DEBUG] Send aux buffer with ID %#016x", auxBuf.id)
			w.auxBufSendChan <- auxBuf
			protocol.FillAuxBufferDataInfo(message, auxBuf.id)
		}

		outputChan, err := w.sendSharedLogMessage(id, message)
		if err != nil {
			return 0, err
		}

		response := <-outputChan
		result := protocol.GetSharedLogResultTypeFromMessage(response)
		if result == protocol.SharedLogResultType_APPEND_OK {
			return protocol.GetLogSeqNumFromMessage(response), nil
		} else if result == protocol.SharedLogResultType_DISCARDED {
			log.Printf("[ERROR] Append discarded, will retry")
			if remainingRetries > 0 {
				time.Sleep(sleepDuration)
				sleepDuration *= 2
				remainingRetries--
				continue
			} else {
				return 0, fmt.Errorf("Failed to append log")
			}
		} else {
			return 0, fmt.Errorf("Failed to append log")
		}
	}
}

func (w *FuncWorker) buildLogEntryFromReadResponse(response []byte) *types.LogEntry {
	seqNum := protocol.GetLogSeqNumFromMessage(response)
	numTags := protocol.GetLogNumTagsFromMessage(response)
	auxDataSize := protocol.GetLogAuxDataSizeFromMessage(response)

	var encodedData []byte
	auxBufId := protocol.GetAuxBufferIdFromMessage(response)
	if auxBufId == protocol.InvalidAuxBufferId {
		encodedData = protocol.GetInlineDataFromMessage(response)
	} else {
		ch := w.getAuxBufferChan(auxBufId)
		// log.Printf("[DEBUG] Waiting aux buffer with ID %#016x", auxBufId)
		auxBuf := <-ch
		encodedData = auxBuf.data
	}

	logDataSize := len(encodedData) - numTags*protocol.SharedLogTagByteSize - auxDataSize
	if logDataSize <= 0 {
		log.Fatalf("[FATAL] Size of inline data too smaler: size=%d, num_tags=%d, aux_data=%d", len(encodedData), numTags, auxDataSize)
	}
	tags := make([]uint64, numTags)
	for i := 0; i < numTags; i++ {
		tags[i] = protocol.GetLogTagFromMessage(response, i)
	}
	logDataStart := numTags * protocol.SharedLogTagByteSize
	return &types.LogEntry{
		SeqNum:  seqNum,
		Tags:    tags,
		Data:    encodedData[logDataStart : logDataStart+logDataSize],
		AuxData: encodedData[logDataStart+logDataSize:],
	}
}

func (w *FuncWorker) sharedLogReadCommon(ctx context.Context, message []byte, opId uint64) (*types.LogEntry, error) {
	// count := atomic.AddInt32(&w.sharedLogReadCount, int32(1))
	// if count > 16 {
	// 	log.Printf("[WARN] Make %d-th shared log read request", count)
	// }

	outputChan, err := w.sendSharedLogMessage(opId, message)
	if err != nil {
		return nil, err
	}

	var response []byte
	select {
	case <-ctx.Done():
		return nil, nil
	case response = <-outputChan:
	}
	result := protocol.GetSharedLogResultTypeFromMessage(response)
	if result == protocol.SharedLogResultType_READ_OK {
		return w.buildLogEntryFromReadResponse(response), nil
	} else if result == protocol.SharedLogResultType_EMPTY {
		return nil, nil
	} else {
		return nil, fmt.Errorf("Failed to read log")
	}
}

func (w *FuncWorker) sharedLogTrimCommon(ctx context.Context, tag uint64, seqNum uint64) error {
	id, currentCallId := w.allocNextLogOp()
	message := protocol.NewSharedLogTrimMessage(currentCallId, w.clientId, tag, seqNum, id)

	outputChan, err := w.sendSharedLogMessage(id, message)
	if err != nil {
		return err
	}

	response := <-outputChan
	result := protocol.GetSharedLogResultTypeFromMessage(response)
	if result == protocol.SharedLogResultType_TRIM_OK {
		return nil
	} else {
		return fmt.Errorf("Failed to trim the log until seqnum %#016x", seqNum)
	}
}

// Implement types.Environment
func (w *FuncWorker) GenerateUniqueID() uint64 {
	uidLowHalf := atomic.AddUint32(&w.nextUidLowHalf, 1)
	return (uint64(w.uidHighHalf) << 32) + uint64(uidLowHalf)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogReadNext(ctx context.Context, tag uint64, seqNum uint64) (*types.LogEntry, error) {
	id, currentCallId := w.allocNextLogOp()
	message := protocol.NewSharedLogReadMessage(currentCallId, w.clientId, tag, seqNum, 1 /* direction */, false /* block */, id)
	return w.sharedLogReadCommon(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogReadNextBlock(ctx context.Context, tag uint64, seqNum uint64) (*types.LogEntry, error) {
	id, currentCallId := w.allocNextLogOp()
	message := protocol.NewSharedLogReadMessage(currentCallId, w.clientId, tag, seqNum, 1 /* direction */, true /* block */, id)
	return w.sharedLogReadCommon(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogReadPrev(ctx context.Context, tag uint64, seqNum uint64) (*types.LogEntry, error) {
	id, currentCallId := w.allocNextLogOp()
	message := protocol.NewSharedLogReadMessage(currentCallId, w.clientId, tag, seqNum, -1 /* direction */, false /* block */, id)
	return w.sharedLogReadCommon(ctx, message, id)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogCheckTail(ctx context.Context, tag uint64) (*types.LogEntry, error) {
	return w.SharedLogReadPrev(ctx, tag, protocol.MaxLogSeqnum)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogTrim(ctx context.Context, seqNum uint64) error {
	return w.sharedLogTrimCommon(ctx, 0 /* tag */, seqNum)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogTrimForTag(ctx context.Context, tag uint64, seqNum uint64) error {
	if tag == 0 || ^tag == 0 {
		return fmt.Errorf("Invalid tag: %v", tag)
	}
	return w.sharedLogTrimCommon(ctx, tag, seqNum)
}

// Implement types.Environment
func (w *FuncWorker) SharedLogSetAuxData(ctx context.Context, seqNum uint64, auxData []byte) error {
	if len(auxData) == 0 {
		return fmt.Errorf("Auxiliary data cannot be empty")
	}

	id, currentCallId := w.allocNextLogOp()
	message := protocol.NewSharedLogSetAuxDataMessage(currentCallId, w.clientId, seqNum, id)

	if len(auxData) <= protocol.MessageInlineDataSize {
		protocol.FillInlineDataInMessage(message, auxData)
	} else {
		auxBuf := &AuxBuffer{
			id:   w.GenerateUniqueID(),
			data: auxData,
		}
		// log.Printf("[DEBUG] Send aux buffer with ID %#016x", auxBuf.id)
		w.auxBufSendChan <- auxBuf
		protocol.FillAuxBufferDataInfo(message, auxBuf.id)
	}

	outputChan, err := w.sendSharedLogMessage(id, message)
	if err != nil {
		return err
	}

	response := <-outputChan
	result := protocol.GetSharedLogResultTypeFromMessage(response)
	if result == protocol.SharedLogResultType_AUXDATA_OK {
		return nil
	} else {
		return fmt.Errorf("Failed to set auxiliary data for log (seqnum %#016x)", seqNum)
	}
}
