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
	"log"
	"sync"

	"go.etcd.io/etcd/etcdserver/api/snap"
)

type kv struct {
	val          string
	txnPhase     string
	writeInternt string //Write intent per key
}

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	kvStore     map[string]kv // current committed key-value pairs
	snapshotter *snap.Snapshotter
	txnPhase    string // "Locked"/ "Prepared" / "Committed" / "Abort"
	writeIntent string // Write intent for the entire store/ shard
}

type operation struct {
	optype string // PUT, GET
	key    int
	val    int
}

//   call(OP)
// TM =====>   KVSTORE
type OP struct {
	cmd  string // Prep, Lock, Commit, Abort
	txId int
	op   []operation //
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]kv), snapshotter: snapshotter}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Lock(operation OP) {

}

func (s *kvstore) Prep(operation OP) {

}

func (s *kvstore) Commit(operation OP) {

}

func (s *kvstore) Abort(operation OP) {

}

func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.kvStore[key]
	return v.val, ok
}

func (s *kvstore) Propose(op OP) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(op); err != nil {
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

		var operation OP
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&operation); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		switch operation.cmd {
		case "Lock":
			s.Lock(operation)
		case "Prep":
			s.Prep(operation)
		case "Commit":
			s.Commit(operation)
		case "Abort":
			s.Abort(operation)
		}
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}

/*func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return json.Marshal(s.kvStore)
}

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
