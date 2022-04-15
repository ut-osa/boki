package worker

import (
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"sync/atomic"

	config "cs.utexas.edu/zjia/faas/config"
	ipc "cs.utexas.edu/zjia/faas/ipc"
	protocol "cs.utexas.edu/zjia/faas/protocol"
)

const PIPE_BUF = 4096

func (w *FuncWorker) writeOutputToShm(funcCall protocol.FuncCall, output []byte) error {
	shmName := ipc.GetFuncCallOutputShmName(funcCall.FullCallId())
	outputRegion, err := ipc.ShmCreate(shmName, len(output))
	if err != nil {
		return err
	}
	defer outputRegion.Close()
	copy(outputRegion.Data, output)
	return nil
}

func (w *FuncWorker) writeOutputToFifo(funcCall protocol.FuncCall, success bool, output []byte) error {
	fifo, err := ipc.FifoOpenForWrite(ipc.GetFuncCallOutputFifoName(funcCall.FullCallId()), true)
	if err != nil {
		return err
	}
	defer fifo.Close()
	var buffer []byte
	if success {
		if len(output)+4 > PIPE_BUF {
			err := w.writeOutputToShm(funcCall, output)
			if err != nil {
				return err
			}
			buffer = make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(len(output)))
		} else {
			buffer = make([]byte, len(output)+4)
			binary.LittleEndian.PutUint32(buffer[0:4], uint32(len(output)))
			copy(buffer[4:], output)
		}
	} else {
		buffer = make([]byte, 4)
		header := int32(-1)
		binary.LittleEndian.PutUint32(buffer, uint32(header))
	}
	_, err = fifo.Write(buffer)
	return err
}

func (w *FuncWorker) newFuncCallCommon(funcCall protocol.FuncCall, input []byte, async bool) ([]byte, error) {
	if async && w.useFifoForNestedCall {
		log.Fatalf("[FATAL] Unsupported")
	}

	message := protocol.NewInvokeFuncCallMessage(funcCall, atomic.LoadUint64(&w.currentCall), async)

	var inputRegion *ipc.ShmRegion
	var outputFifo *os.File
	var outputChan chan []byte
	var output []byte
	var err error

	if len(input) > protocol.MessageInlineDataSize {
		inputRegion, err = ipc.ShmCreate(ipc.GetFuncCallInputShmName(funcCall.FullCallId()), len(input))
		if err != nil {
			return nil, fmt.Errorf("ShmCreate failed: %v", err)
		}
		defer func() {
			inputRegion.Close()
			if !async {
				inputRegion.Remove()
			}
		}()
		copy(inputRegion.Data, input)
		protocol.SetPayloadSizeInMessage(message, int32(-len(input)))
	} else {
		protocol.FillInlineDataInMessage(message, input)
	}

	if w.useFifoForNestedCall {
		outputFifoName := ipc.GetFuncCallOutputFifoName(funcCall.FullCallId())
		err = ipc.FifoCreate(outputFifoName)
		if err != nil {
			return nil, fmt.Errorf("FifoCreate failed: %v", err)
		}
		defer ipc.FifoRemove(outputFifoName)
		outputFifo, err = ipc.FifoOpenForReadWrite(outputFifoName, true)
		if err != nil {
			return nil, fmt.Errorf("FifoOpenForReadWrite failed: %v", err)
		}
		defer outputFifo.Close()
	}

	w.mux.Lock()
	if !w.useFifoForNestedCall {
		outputChan = make(chan []byte, 1)
		w.outgoingFuncCalls[funcCall.FullCallId()] = outputChan
	}
	_, err = w.outputPipe.Write(message)
	w.mux.Unlock()

	if w.useFifoForNestedCall {
		headerBuf := make([]byte, 4)
		nread, err := outputFifo.Read(headerBuf)
		if err != nil {
			return nil, fmt.Errorf("Failed to read from fifo: %v", err)
		} else if nread < len(headerBuf) {
			return nil, fmt.Errorf("Failed to read header from output fifo")
		}

		header := int32(binary.LittleEndian.Uint32(headerBuf))
		if header < 0 {
			return nil, fmt.Errorf("FuncCall failed")
		}

		outputSize := int(header)
		output = make([]byte, outputSize)
		if outputSize+4 > PIPE_BUF {
			outputRegion, err := ipc.ShmOpen(ipc.GetFuncCallOutputShmName(funcCall.FullCallId()), true)
			if err != nil {
				return nil, fmt.Errorf("ShmOpen failed: %v", err)
			}
			defer func() {
				outputRegion.Close()
				outputRegion.Remove()
			}()
			if outputRegion.Size != outputSize {
				return nil, fmt.Errorf("Shm size mismatch with header read from output fifo")
			}
			copy(output, outputRegion.Data)
		} else {
			nread, err = outputFifo.Read(output)
			if err != nil {
				return nil, fmt.Errorf("Failed to read from fifo: %v", err)
			} else if nread < outputSize {
				return nil, fmt.Errorf("Failed to read output from fifo")
			}
		}
	} else {
		message := <-outputChan
		if async {
			return nil, nil
		}
		if protocol.IsFuncCallFailedMessage(message) {
			return nil, fmt.Errorf("FuncCall failed")
		}
		payloadSize := protocol.GetPayloadSizeFromMessage(message)
		if payloadSize < 0 {
			outputSize := int(-payloadSize)
			output = make([]byte, outputSize)
			outputRegion, err := ipc.ShmOpen(ipc.GetFuncCallOutputShmName(funcCall.FullCallId()), true)
			if err != nil {
				return nil, fmt.Errorf("ShmOpen failed: %v", err)
			}
			defer func() {
				outputRegion.Close()
				outputRegion.Remove()
			}()
			if outputRegion.Size != outputSize {
				return nil, fmt.Errorf("Shm size mismatch with header read from output fifo")
			}
			copy(output, outputRegion.Data)
		} else {
			output = protocol.GetInlineDataFromMessage(message)
		}
	}

	return output, nil
}

// Implement types.Environment
func (w *FuncWorker) InvokeFunc(ctx context.Context, funcName string, input []byte) ([]byte, error) {
	entry := config.FindByFuncName(funcName)
	if entry == nil {
		return nil, fmt.Errorf("Invalid function name: %s", funcName)
	}
	funcCall := protocol.FuncCall{
		FuncId:   entry.FuncId,
		MethodId: 0,
		ClientId: w.clientId,
		CallId:   atomic.AddUint32(&w.nextCallId, 1) - 1,
	}
	return w.newFuncCallCommon(funcCall, input, false /* async */)
}

// Implement types.Environment
func (w *FuncWorker) InvokeFuncAsync(ctx context.Context, funcName string, input []byte) error {
	entry := config.FindByFuncName(funcName)
	if entry == nil {
		return fmt.Errorf("Invalid function name: %s", funcName)
	}
	funcCall := protocol.FuncCall{
		FuncId:   entry.FuncId,
		MethodId: 0,
		ClientId: w.clientId,
		CallId:   atomic.AddUint32(&w.nextCallId, 1) - 1,
	}
	_, err := w.newFuncCallCommon(funcCall, input, true /* async */)
	return err
}

// Implement types.Environment
func (w *FuncWorker) GrpcCall(ctx context.Context, service string, method string, request []byte) ([]byte, error) {
	entry := config.FindByFuncName("grpc:" + service)
	if entry == nil {
		return nil, fmt.Errorf("Invalid gRPC service: %s", service)
	}
	methodId := entry.FindGrpcMethod(method)
	if methodId < 0 {
		return nil, fmt.Errorf("Invalid gRPC method: %s", method)
	}
	funcCall := protocol.FuncCall{
		FuncId:   entry.FuncId,
		MethodId: uint16(methodId),
		ClientId: w.clientId,
		CallId:   atomic.AddUint32(&w.nextCallId, 1) - 1,
	}
	return w.newFuncCallCommon(funcCall, request, false /* async */)
}
