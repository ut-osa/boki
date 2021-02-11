package statestore

import (
	"hash/fnv"

	gabs "github.com/Jeffail/gabs/v2"
)

type ObjectView struct {
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
}

func objectNameHash(name string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(name))
	return h.Sum64()
}

func (env *envImpl) Object(name string) *ObjectRef {
	if env.txnCtx != nil && !env.txnCtx.active {
		panic("Cannot create object within inactive transaction!")
	}
	obj := &ObjectRef{
		env:      env,
		name:     name,
		nameHash: objectNameHash(name),
		view:     nil,
		multiCtx: nil,
		txnCtx:   env.txnCtx,
	}
	env.objs = append(env.objs, obj)
	return obj
}

func (obj *ObjectRef) ensureView() error {
	if obj.view == nil {
		if obj.txnCtx == nil {
			return obj.Sync()
		} else {
			return obj.syncTo(obj.txnCtx.id)
		}
	} else {
		return nil
	}
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
