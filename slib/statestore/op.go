package statestore

import (
	gabs "github.com/Jeffail/gabs/v2"
)

const (
	OP_Set = iota
	OP_MakeObject
	OP_MakeArray
	OP_Delete
	OP_NumberFetchAdd
	OP_ArrayPushBack
	OP_ArrayPushBackWithLimit
	OP_ArrayPopBack
)

type WriteOp struct {
	OpType   int    `json:"t"`
	ObjName  string `json:"o"`
	Path     string `json:"p"`
	Value    Value  `json:"v"`
	IntParam int    `json:"i"`
}

type WriteResult struct {
	filled bool
	Value  Value
	Err    error
}

type multiContext struct {
	ops     []*WriteOp
	results []*WriteResult
}

func newEmptyResult() *WriteResult {
	return &WriteResult{
		filled: false,
		Value:  NullValue(),
		Err:    nil,
	}
}

func (r *WriteResult) success() *WriteResult {
	if r.filled {
		panic("Already filled")
	}
	r.filled = true
	return r
}

func (r *WriteResult) successWithValue(value Value) *WriteResult {
	if r.filled {
		panic("Already filled")
	}
	r.filled = true
	r.Value = value
	return r
}

func (r *WriteResult) failure(err error) *WriteResult {
	if r.filled {
		panic("Already filled")
	}
	r.filled = true
	r.Err = err
	return r
}

func (ctx *multiContext) appendOp(op *WriteOp, result *WriteResult) {
	ctx.ops = append(ctx.ops, op)
	ctx.results = append(ctx.results, result)
}

func (obj *ObjectRef) MultiBegin() {
	if obj.multiCtx != nil {
		panic("Already in multi context")
	}
	if obj.txnCtx != nil {
		panic("Multi not supported within transaction")
	}
	obj.multiCtx = &multiContext{
		ops:     make([]*WriteOp, 0, 4),
		results: make([]*WriteResult, 0, 4),
	}
}

func (obj *ObjectRef) MultiAbort() {
	if obj.multiCtx == nil {
		panic("Not in multi context")
	}
	obj.multiCtx = nil
}

func (obj *ObjectRef) MultiCommit() error {
	if obj.multiCtx == nil {
		panic("Not in multi context")
	}
	ctx := obj.multiCtx
	obj.multiCtx = nil
	if len(ctx.ops) == 0 {
		// Nothing to commit
		return nil
	}
	seqNum, err := obj.appendNormalOpLog(ctx.ops)
	if err != nil {
		return err
	}
	if err := obj.syncTo(seqNum); err != nil {
		return err
	}
	obj.view.nextSeqNum = seqNum + 1
	for i, op := range ctx.ops {
		result := ctx.results[i]
		if value, err := obj.view.applyWriteOp(op); err != nil {
			result.failure(err)
		} else {
			result.successWithValue(value)
		}
	}
	return nil
}

func (obj *ObjectRef) doWriteOp(op *WriteOp) *WriteResult {
	if op.Value.Object != nil || op.Value.Array != nil {
		panic("Object or Array value not supported")
	}
	result := newEmptyResult()
	if obj.multiCtx != nil {
		obj.multiCtx.appendOp(op, result)
		return result
	}
	if obj.txnCtx != nil {
		if !obj.txnCtx.active {
			panic("Cannot do modifications within inactive transaction!")
		}
		if obj.txnCtx.readonly {
			panic("Cannot do modifications within read-only transaction!")
		}
		obj.txnCtx.appendOp(obj, op)
		result.successWithValue(NullValue())
		return result
	}
	seqNum, err := obj.appendWriteLog(op)
	if err != nil {
		return result.failure(err)
	}
	if err := obj.syncTo(seqNum); err != nil {
		return result.failure(err)
	}
	obj.view.nextSeqNum = seqNum + 1
	if value, err := obj.view.applyWriteOp(op); err != nil {
		return result.failure(err)
	} else {
		return result.successWithValue(value)
	}
}

func (obj *ObjectRef) Set(path string, value Value) *WriteResult {
	return obj.doWriteOp(&WriteOp{
		OpType:  OP_Set,
		ObjName: obj.name,
		Path:    path,
		Value:   value,
	})
}

func (obj *ObjectRef) SetString(path string, value string) *WriteResult {
	return obj.Set(path, StringValue(value))
}

func (obj *ObjectRef) SetNumber(path string, value float64) *WriteResult {
	return obj.Set(path, NumberValue(value))
}

func (obj *ObjectRef) SetBoolean(path string, value bool) *WriteResult {
	return obj.Set(path, BoolValue(value))
}

func (obj *ObjectRef) MakeObject(path string) *WriteResult {
	return obj.doWriteOp(&WriteOp{
		OpType:  OP_MakeObject,
		ObjName: obj.name,
		Path:    path,
		Value:   NullValue(),
	})
}

func (obj *ObjectRef) MakeArray(path string, arraySize int) *WriteResult {
	if arraySize < 0 {
		panic("Negative arraySize")
	}
	return obj.doWriteOp(&WriteOp{
		OpType:   OP_MakeArray,
		ObjName:  obj.name,
		Path:     path,
		IntParam: arraySize,
	})
}

func (obj *ObjectRef) Delete(path string) *WriteResult {
	return obj.doWriteOp(&WriteOp{
		OpType:  OP_Delete,
		ObjName: obj.name,
		Path:    path,
		Value:   NullValue(),
	})
}

func (obj *ObjectRef) NumberFetchAdd(path string, delta float64) *WriteResult {
	return obj.doWriteOp(&WriteOp{
		OpType:  OP_NumberFetchAdd,
		ObjName: obj.name,
		Path:    path,
		Value:   NumberValue(delta),
	})
}

func (obj *ObjectRef) ArrayPushBack(path string, value Value) *WriteResult {
	return obj.doWriteOp(&WriteOp{
		OpType:  OP_ArrayPushBack,
		ObjName: obj.name,
		Path:    path,
		Value:   value,
	})
}

func (obj *ObjectRef) ArrayPushBackWithLimit(path string, value Value, sizeLimit int) *WriteResult {
	if sizeLimit < 0 {
		panic("Negative sizeLimit")
	}
	return obj.doWriteOp(&WriteOp{
		OpType:   OP_ArrayPushBackWithLimit,
		ObjName:  obj.name,
		Path:     path,
		Value:    value,
		IntParam: sizeLimit,
	})
}

func (obj *ObjectRef) ArrayPopBack(path string) *WriteResult {
	return obj.doWriteOp(&WriteOp{
		OpType:  OP_ArrayPopBack,
		ObjName: obj.name,
		Path:    path,
		Value:   NullValue(),
	})
}

func applySetOp(parent *gabs.Container, lastSeg string, value interface{}) (Value /* oldValue */, error) {
	if current := parent.Search(lastSeg); current != nil {
		oldValue := valueFromInterface(current.Data())
		if _, err := parent.Set(value, lastSeg); err == nil {
			return oldValue, nil
		} else {
			return NullValue(), newGabsError(err)
		}
	} else {
		if _, err := parent.Set(value, lastSeg); err == nil {
			return NullValue(), nil
		} else {
			return NullValue(), newGabsError(err)
		}
	}
}

func applyMakeObjectOp(parent *gabs.Container, lastSeg string) error {
	if parent.Exists(lastSeg) {
		return newPathConflictError(lastSeg)
	}
	_, err := parent.Object(lastSeg)
	if err != nil {
		return newGabsError(err)
	}
	return nil
}

func applyMakeArrayOp(parent *gabs.Container, lastSeg string, arraySize int) error {
	if parent.Exists(lastSeg) {
		return newPathConflictError(lastSeg)
	}
	_, err := parent.ArrayOfSize(arraySize, lastSeg)
	if err != nil {
		return newGabsError(err)
	}
	return nil
}

func applyDeleteOp(parent *gabs.Container, lastSeg string) (Value /* deletedValue */, error) {
	current := parent.Search(lastSeg)
	if current == nil {
		return NullValue(), newPathNotExistError(lastSeg)
	}
	value := valueFromInterface(current.Data())
	if err := current.Delete(lastSeg); err == nil {
		return value, nil
	} else {
		return NullValue(), newGabsError(err)
	}
}

func applyNumberFetchAddOp(parent *gabs.Container, lastSeg string, delta float64) (Value /* oldValue */, error) {
	current := parent.Search(lastSeg)
	if current == nil {
		if _, err := parent.Set(delta, lastSeg); err == nil {
			return NumberValue(0), nil
		} else {
			return NullValue(), newGabsError(err)
		}
	}
	if value, ok := current.Data().(float64); ok {
		oldValue := NumberValue(value)
		if _, err := parent.Set(value+delta, lastSeg); err == nil {
			return oldValue, nil
		} else {
			return NullValue(), newGabsError(err)
		}
	} else {
		return NullValue(), newBadArgumentsError("Expect number type")
	}
}

func applyArrayPushBackOp(parent *gabs.Container, lastSeg string, value interface{}) error {
	current := parent.Search(lastSeg)
	if current == nil {
		return newPathNotExistError(lastSeg)
	}
	if arr, ok := current.Data().([]interface{}); ok {
		newArr := append(arr, value)
		if _, err := parent.Set(newArr, lastSeg); err == nil {
			return nil
		} else {
			return newGabsError(err)
		}
	} else {
		return newBadArgumentsError("Expect array type")
	}
}

func applyArrayPushBackWithLimitOp(parent *gabs.Container, lastSeg string, value interface{}, sizeLimit int) (Value /* oldValue */, error) {
	current := parent.Search(lastSeg)
	if current == nil {
		return NullValue(), newPathNotExistError(lastSeg)
	}
	if arr, ok := current.Data().([]interface{}); ok {
		oldValue := NullValue()
		newArr := append(arr, value)
		if len(newArr) > sizeLimit {
			oldValue = valueFromInterface(arr[0])
			newArr = arr[1:]
		}
		if _, err := parent.Set(newArr, lastSeg); err == nil {
			return oldValue, nil
		} else {
			return NullValue(), newGabsError(err)
		}
	} else {
		return NullValue(), newBadArgumentsError("Expect array type")
	}
}

func applyArrayPopBackOp(parent *gabs.Container, lastSeg string) (Value, error) {
	current := parent.Search(lastSeg)
	if current == nil {
		return NullValue(), newPathNotExistError(lastSeg)
	}
	if arr, ok := current.Data().([]interface{}); ok {
		if len(arr) == 0 {
			return NullValue(), newBadArgumentsError("Empty array")
		}
		value := valueFromInterface(arr[len(arr)-1])
		newArr := arr[:len(arr)-1]
		if _, err := parent.Set(newArr, lastSeg); err == nil {
			return value, nil
		} else {
			return NullValue(), newGabsError(err)
		}
	} else {
		return NullValue(), newBadArgumentsError("Expect array type")
	}
}

func (view *ObjectView) applyWriteOp(op *WriteOp) (Value, error) {
	pathSegs := gabs.DotPathToSlice(op.Path)
	if len(pathSegs) == 0 {
		return NullValue(), newPathNotExistError(op.Path)
	}
	parent := view.contents.Search(pathSegs[:len(pathSegs)-1]...)
	if parent == nil {
		return NullValue(), newPathNotExistError(op.Path)
	}
	lastSeg := pathSegs[len(pathSegs)-1]
	switch op.OpType {
	case OP_Set:
		return applySetOp(parent, lastSeg, op.Value.asInterface())
	case OP_MakeObject:
		return NullValue(), applyMakeObjectOp(parent, lastSeg)
	case OP_MakeArray:
		return NullValue(), applyMakeArrayOp(parent, lastSeg, op.IntParam)
	case OP_Delete:
		return applyDeleteOp(parent, lastSeg)
	case OP_NumberFetchAdd:
		return applyNumberFetchAddOp(parent, lastSeg, op.Value.AsNumber())
	case OP_ArrayPushBack:
		return NullValue(), applyArrayPushBackOp(parent, lastSeg, op.Value.asInterface())
	case OP_ArrayPushBackWithLimit:
		return applyArrayPushBackWithLimitOp(parent, lastSeg, op.Value.asInterface(), op.IntParam)
	case OP_ArrayPopBack:
		return applyArrayPopBackOp(parent, lastSeg)
	default:
		panic("Unknown WriteOp!")
	}
	return NullValue(), nil
}
