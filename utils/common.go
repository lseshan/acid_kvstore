package utils

import (
	"hash/fnv"
)

func keytoshard(key string) uint64 {
	h := fnv.New64()
	h.Write([]byte(key))
	return h.Sum64()
}
