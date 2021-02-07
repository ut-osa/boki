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
}

func objectNameHash(name string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(name))
	return h.Sum64()
}

func (env *envImpl) Object(name string) *ObjectRef {
	return &ObjectRef{
		env:      env,
		name:     name,
		nameHash: objectNameHash(name),
		view:     nil,
	}
}

func (obj *ObjectRef) ensureView() error {
	if obj.view == nil {
		return obj.Sync()
	} else {
		return nil
	}
}

func (obj *ObjectRef) Get(path string) (Value, error) {
	if err := obj.ensureView(); err != nil {
		return NullValue(), err
	}
	resolved := obj.view.contents.Path(path)
	if resolved == nil {
		return NullValue(), newPathNotExistError(path)
	}
	return valueFromInterface(resolved.Data()), nil
}
