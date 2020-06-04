package utils

import (
	"hash/fnv"
)

func Keytoshard(key string) uint64 {
	h := fnv.New64()
	h.Write([]byte(key))
	return h.Sum64()
}
