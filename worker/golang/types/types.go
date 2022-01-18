package types

import (
	"context"
)

type LogEntry struct {
	SeqNum  uint64
	Tags    []uint64
	Data    []byte
	AuxData []byte
}

type Environment interface {
	InvokeFunc(ctx context.Context, funcName string, input []byte) ( /* output */ []byte, error)
	InvokeFuncAsync(ctx context.Context, funcName string, input []byte) error
	GrpcCall(ctx context.Context, service string, method string, request []byte) ( /* reply */ []byte, error)

	GenerateUniqueID() uint64

	// Shared log operations
	// Append a new log entry, tags must be non-zero
	SharedLogAppend(ctx context.Context, tags []uint64, data []byte) ( /* seqnum */ uint64, error)
	// Read the first log with `tag` whose seqnum >= given `seqNum`
	// `tag`==0 means considering log with any tag, including empty tag
	SharedLogReadNext(ctx context.Context, tag uint64, seqNum uint64) (*LogEntry, error)
	SharedLogReadNextBlock(ctx context.Context, tag uint64, seqNum uint64) (*LogEntry, error)
	// Read the last log with `tag` whose seqnum <= given `seqNum`
	// `tag`==0 means considering log with any tag, including empty tag
	SharedLogReadPrev(ctx context.Context, tag uint64, seqNum uint64) (*LogEntry, error)
	// Alias for ReadPrev(tag, MaxSeqNum)
	SharedLogCheckTail(ctx context.Context, tag uint64) (*LogEntry, error)
	// Trim the log whose seqnum < `seqnum`
	SharedLogTrim(ctx context.Context, tag uint64, seqNum uint64) error
	// Set auxiliary data for log entry of given `seqNum`
	SharedLogSetAuxData(ctx context.Context, seqNum uint64, auxData []byte) error
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
