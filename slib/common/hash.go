package common

import (
	"hash/fnv"
)

func NameHash(name string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(name))
	return h.Sum64()
}
