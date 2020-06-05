package utils

import (
	"hash/fnv"
)

func Keytoshard(key string, nshards int) uint64 {
	h := fnv.New64()
	h.Write([]byte(key))
	return (h.Sum64() % uint64(nshards)) + 1
}
