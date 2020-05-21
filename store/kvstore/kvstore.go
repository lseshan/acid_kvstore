// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kvstore

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/etcdserver/api/snap"
)

// Key exists
/*Key 'A': { val : 10
	       writeIntent: 20
	     }
Commit: Once raft majority comes back
Commit(key string) {
  v = kvstore['A']
  v.mu.RLock()
  v.val = v.writeIntent
  v.writeIntent = ""
  v.mu.RUnLock()
  //Write to disk (v)
  print(v.val) ===> 20
}*/
var txnMap map[uint64]Txn

type value struct {
	mu  sync.RWMutex
	val string
	//txnPhase     string
	//val_shdw     string //Empty
	writeIntent string //Write intent per key
	txnId       int
}

//Kvstore exported for use by other packages
type Kvstore = kvstore

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	KvStore     map[string]value // current committed key-value pairs
	snapshotter *snap.Snapshotter
	txnPhase    string      // "Locked"/ "Prepared" / "Committed" / "Abort"
	writeIntent []operation // Write intent for the entire store/ shard
	txnId       int
}

type KV struct {
	Key string `json:"key"`
	Val string `json:"val"`
}

type operation struct {
	Optype string // PUT, GET
	Key    string
	Val    string
}

//   call(OP)
// TM =====>   KVSTORE
type Txn struct {
	Cmd    string // Prep, Lock, Commit, Abort
	TxId   uint64
	Oper   []operation //
	RespCh chan<- int
}

type raftMsg struct {
	MsgType string
	Rawkv   operation
	Txn     Txn
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, KvStore: make(map[string]value), snapshotter: snapshotter}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Put(kv operation) {
	log.Printf("Put value")
	var v value
	v.val = kv.Val
	s.KvStore[kv.Key] = v
}

// Locks the db
func (s *kvstore) Lock(txn Txn) {

}

func (s *kvstore) Prep(txn Txn) {
	log.Printf("Got Prep")

	//Check for locks
	s.txnPhase = "Prep"
	s.writeIntent = txn.Oper
	for _, oper := range txn.Oper {
		//Should we take lock
		key := s.KvStore[oper.Key]
		key.writeIntent = oper.Val
		s.KvStore[oper.Key] = key
	}
	log.Printf("Sending commit response")
	if stxn, found := txnMap[txn.TxId]; found {
		stxn.RespCh <- 1
	}
	delete(txnMap, txn.TxId)
}

func (s *kvstore) Commit(txn Txn) {
	s.txnPhase = "Commit"
	for _, oper := range txn.Oper {
		//Should we take a lock
		value := s.KvStore[oper.Key]
		value.val = value.writeIntent
		value.writeIntent = ""
		s.KvStore[oper.Key] = value
	}
	oper := txn.Oper[0]
	log.Printf("key : %v val : %v", oper.Key, s.KvStore[oper.Key].val)
	s.writeIntent = []operation{}
	log.Printf("Sending response")
	if stxn, found := txnMap[txn.TxId]; found {
		stxn.RespCh <- 1
	}
	delete(txnMap, txn.TxId)
	//Snapshot
}

func (s *kvstore) Abort(txn Txn) {
	s.txnPhase = "Abort"
	for _, oper := range txn.Oper {
		//Should we take a lokc
		value := s.KvStore[oper.Key]
		value.writeIntent = ""
	}
	s.writeIntent = []operation{}
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.KvStore[key]
	return v.val, ok
}

func (s *kvstore) ProposeKV(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(raftMsg{MsgType: "raw", Rawkv: operation{"Upd", k, v}, Txn: Txn{}}); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) ProposeTxn(txn Txn) {
	var buf bytes.Buffer
	txnMap[txn.TxId] = txn
	if err := gob.NewEncoder(&buf).Encode(raftMsg{MsgType: "txn", Rawkv: operation{}, Txn: txn}); err != nil {
		log.Fatal(err)
	}
	log.Printf("propose txn")
	if txn.RespCh != nil {
		log.Printf("RespC is not  nil %v", txn.RespCh)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) HandleKVOperation(key string, val string, op string) KV {
	var kv KV
	switch op {
	case "GET":
		log.Printf("Got get")
		kv.Key = key
		kv.Val = s.KvStore[key].val
		log.Printf("%v", s.KvStore[key])
	case "PUT":
		fallthrough
	case "POST":
		s.ProposeKV(key, val)
	case "DEL":
		delete(s.KvStore, key)
	}
	return kv
}

func (s *kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			/*if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}*/
			continue
		}

		var msg raftMsg
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		log.Printf("%v", dec)
		if err := dec.Decode(&msg); err != nil {
			log.Fatalf("kvstore: could not decode message (%v)", err)
		}
		s.mu.Lock()
		switch msg.MsgType {
		case "txn":
			log.Printf("Got txn from raft")
			switch msg.Txn.Cmd {
			case "Lock":
				s.Lock(msg.Txn)
			case "Prep":
				s.Prep(msg.Txn)
			case "Commit":
				s.Commit(msg.Txn)
			case "Abort":
				s.Abort(msg.Txn)
			}
		case "raw":
			switch msg.Rawkv.Optype {
			case "Create":
				fallthrough
			case "Upd":
				s.Put(msg.Rawkv)
			}
		}
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

func (s *kvstore) GetSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.KvStore)
}

/*
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kvStore = store
	return nil
}*/
func init() {
	txnMap = make(map[uint64]Txn)
}
