package statestore

const (
	OP_Set = iota
	OP_Delete
	OP_NumberAddFetch
	OP_BoolFlip
	OP_ArrayPushBack
	OP_ArrayPopBack
)

type WriteOp struct {
	OpType  int    `json:"t"`
	ObjName string `json:"o"`
	Path    string `json:"p"`
	Value   Value  `json:"v"`
}

func (obj *ObjectRef) Set(path string, value Value) (Value /* oldValue */, error) {
	op := WriteOp{
		OpType:  OP_Set,
		ObjName: obj.name,
		Path:    path,
		Value:   value,
	}
	seqNum, err := obj.appendWriteLog(&op)
	if err != nil {
		return NullValue(), newRuntimeError(err.Error())
	}
	obj.SyncTo(seqNum)
	oldValue := valueFromInterface(obj.view.contents.Path(path).Data())
	err = obj.view.applyWriteOp(seqNum, &op)
	return oldValue, err
}

func (obj *ObjectRef) MakeArray(path string) error {
	panic("Not implemented")
}

func (obj *ObjectRef) Delete(path string) (Value /* oldValue */, error) {
	panic("Not implemented")
}

func (obj *ObjectRef) NumberAddFetch(path string, delta float64) (float64 /* oldValue */, error) {
	panic("Not implemented")
}

func (obj *ObjectRef) BoolFlip(path string) (bool /* oldValue */, error) {
	panic("Not implemented")
}

func (obj *ObjectRef) ArrayPushBack(path string, value Value) error {
	panic("Not implemented")
}

func (obj *ObjectRef) ArrayPopBack(path string) (Value /* oldValue */, error) {
	panic("Not implemented")
}

func (view *ObjectView) applyWriteOp(seqNum uint64, op *WriteOp) error {
	view.nextSeqNum = seqNum + 1
	switch op.OpType {
	case OP_Set:
		if _, err := view.contents.SetP(op.Value.asInterface(), op.Path); err != nil {
			return newPathNotExistError(op.Path)
		}
	default:
		panic("Not implemented")
	}
	return nil
}
