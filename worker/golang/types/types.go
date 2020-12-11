package types

import (
	"context"
)

type LogEntry struct {
	SeqNum uint64
	Data   []byte
}

type Environment interface {
	InvokeFunc(ctx context.Context, funcName string, input []byte) ( /* output */ []byte, error)
	InvokeFuncAsync(ctx context.Context, funcName string, input []byte) error
	GrpcCall(ctx context.Context, service string, method string, request []byte) ( /* reply */ []byte, error)
	SharedLogAppend(ctx context.Context, tag uint32, data []byte) ( /* seqnum */ uint64, error)
	SharedLogReadNext(ctx context.Context, tag uint32, startSeqNum uint64) (*LogEntry, error)
	SharedLogCheckTail(ctx context.Context, tag uint32) (*LogEntry, error)
}

type FuncHandler interface {
	Call(ctx context.Context, input []byte) ( /* output */ []byte, error)
}

type GrpcFuncHandler interface {
	Call(ctx context.Context, method string, request []byte) ( /* reply */ []byte, error)
}

type FuncHandlerFactory interface {
	New(env Environment, funcName string) (FuncHandler, error)
	GrpcNew(env Environment, service string) (GrpcFuncHandler, error)
}
