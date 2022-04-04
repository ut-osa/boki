package statestore

import (
	"cs.utexas.edu/zjia/faas/slib/common"

	"cs.utexas.edu/zjia/faas/protocol"

	gabs "github.com/Jeffail/gabs/v2"
)

type ObjectView struct {
	name       string
	nextSeqNum uint64
	contents   *gabs.Container
}

type ObjectRef struct {
	env      *envImpl
	name     string
	nameHash uint64
	view     *ObjectView
	multiCtx *multiContext
	txnCtx   *txnContext
	isNew    bool
	logCount int
}

func (env *envImpl) Object(name string) *ObjectRef {
	if env.txnCtx != nil && !env.txnCtx.active {
		panic("Cannot create object within inactive transaction!")
	}
	if obj, exists := env.objs[name]; exists {
		return obj
	} else {
		obj := &ObjectRef{
			env:      env,
			name:     name,
			nameHash: common.NameHash(name),
			view:     nil,
			multiCtx: nil,
			txnCtx:   env.txnCtx,
			isNew:    true,
			logCount: 0,
		}
		env.objs[name] = obj
		return obj
	}
}

func (env *envImpl) DeleteObject(name string) error {
	if env.txnCtx != nil {
		panic("Cannot delete object within a transaction context")
	}
	if err := env.appendDeleteLog(name); err != nil {
		return err
	}
	if _, exists := env.objs[name]; exists {
		delete(env.objs, name)
	}
	return nil
}

func newEmptyObjectView(objName string) *ObjectView {
	return &ObjectView{
		name:       objName,
		nextSeqNum: 0,
		contents:   gabs.New(),
	}
}

func (objView *ObjectView) Clone() *ObjectView {
	return &ObjectView{
		name:       objView.name,
		nextSeqNum: objView.nextSeqNum,
		contents:   gabs.Wrap(common.DeepCopy(objView.contents.Data())),
	}
}

func (obj *ObjectRef) ensureView() error {
	if obj.view == nil {
		tailSeqNum := protocol.MaxLogSeqnum
		if obj.txnCtx != nil {
			tailSeqNum = obj.txnCtx.id
		}
		return obj.syncTo(tailSeqNum)
	} else {
		return nil
	}
}

func (obj *ObjectRef) Sync() error {
	if obj.txnCtx != nil {
		panic("Cannot Sync() objects within a transaction context")
	}
	return obj.syncTo(protocol.MaxLogSeqnum)
}

func (obj *ObjectRef) Get(path string) (Value, error) {
	if obj.multiCtx != nil {
		panic("Cannot call Get within multi context")
	}
	if obj.txnCtx != nil && !obj.txnCtx.active {
		panic("Cannot call Get within inactive transaction!")
	}
	if err := obj.ensureView(); err != nil {
		return NullValue(), err
	}
	resolved := obj.view.contents.Path(path)
	if resolved == nil {
		return NullValue(), newPathNotExistError(path)
	}
	return valueFromInterface(resolved.Data()), nil
}
