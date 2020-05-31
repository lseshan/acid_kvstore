package storage

import (
	"github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/store/utils/engine_util"
)

// Storage represents the internal-facing server part of TinyKV, it handles sending and receiving from other
// TinyKV nodes. As part of that responsibility, it also reads and writes data to disk (or semi-permanent memory).
type Storage interface {
	Start() error
	Stop() error
	Write(ctx *kvstorepb.TxContext, batch []Modify) error
	Reader(ctx *kvstorepb.TxContext) (StorageReader, error)
}

type StorageReader interface {
	// When the key doesn't exist, return nil for the value
	GetCF(cf string, key []byte) ([]byte, error)
	IterCF(cf string) engine_util.DBIterator
	Close()
}
