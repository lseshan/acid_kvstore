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
	kvStore     map[string]value // current committed key-value pairs
	snapshotter *snap.Snapshotter
	txnPhase    string      // "Locked"/ "Prepared" / "Committed" / "Abort"
	writeIntent []operation // Write intent for the entire store/ shard
	txnId       int
}

type operation struct {
	optype string // PUT, GET
	key    string
	val    string
}

//   call(OP)
// TM =====>   KVSTORE
type Txn struct {
	cmd  string // Prep, Lock, Commit, Abort
	txId int
	oper []operation //
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]value), snapshotter: snapshotter}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

// Locks the db
func (s *kvstore) Lock(txn Txn) {

}

func (s *kvstore) Prep(txn Txn) {

	//Check for locks
	s.txnPhase = "Prep"
	s.writeIntent = txn.oper
	for _, oper := range txn.oper {
		//Should we take lock
		key := s.kvStore[oper.key]
		key.writeIntent = oper.val
	}

}

func (s *kvstore) Commit(txn Txn) {
	s.txnPhase = "Commit"
	for _, oper := range txn.oper {
		//Should we take a lock
		value := s.kvStore[oper.key]
		value.val = value.writeIntent
		value.writeIntent = ""
	}
	s.writeIntent = []operation{}
	//Snapshot
}

func (s *kvstore) Abort(txn Txn) {
	s.txnPhase = "Abort"
	for _, oper := range txn.oper {
		//Should we take a lokc
		value := s.kvStore[oper.key]
		value.writeIntent = ""
	}
	s.writeIntent = []operation{}
}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v.val, ok
}

func (s *kvstore) Propose(txn Txn) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(txn); err != nil {
		log.Fatal(err)
	}
	s.proposeC <- buf.String()
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

		var txn Txn
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&txn); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		switch txn.cmd {
		case "Lock":
			s.Lock(txn)
		case "Prep":
			s.Prep(txn)
		case "Commit":
			s.Commit(txn)
		case "Abort":
			s.Abort(txn)
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
	return json.Marshal(s.kvStore)
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
