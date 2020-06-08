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
	"context"
	"encoding/gob"
	"encoding/json"

	//	"log"
	"sync"
	"time"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	replpb "github.com/acid_kvstore/proto/package/replicamgrpb"
	pbt "github.com/acid_kvstore/proto/package/txmanagerpb"
	"github.com/acid_kvstore/raft"
	log "github.com/pingcap-incubator/tinykv/log"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc"
)

const (
	SUCCESS = true
	FAILURE = false
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

type Replica struct {
	Stores      map[uint64]*kvstore //map of kvstores keys by region id/shard id
	Config      *pb.ReplicaConfig
	Conn        *grpc.ClientConn
	Replclient  replpb.ReplicamgrClient
	ReplicaName string
}

type value struct {
	mu  sync.RWMutex
	val string
	//txnPhase     string
	//val_shdw     string //Empty
	writeIntent string //Write intent per key
	txnId       uint64
}

//Kvstore exported for use by other packages
type Kvstore = kvstore

// a key-value store backed by raft
type kvstore struct {
	proposeC    chan<- string // channel for proposing updates
	mu          sync.RWMutex
	KvStore     map[string]*value // current committed key-value pairs
	snapshotter *snap.Snapshotter
	txnPhase    string      // "Locked"/ "Prepared" / "Committed" / "Abort"
	writeIntent []operation // Write intent for the entire store/ shard
	txnId       int
	Node        *raft.RaftNode
	txnMapLock  sync.RWMutex
	txnMap      map[uint64]Txn
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

func (repl *Replica) StartReplMgrGrpcClient() {
	log.Infof("Dialling %v", repl.Config.ReplLeader)
	conn, _ := grpc.Dial(repl.Config.ReplLeader, grpc.WithInsecure(), grpc.WithBlock())
	client := replpb.NewReplicamgrClient(conn)
	repl.Replclient = client
	repl.Conn = conn
}

func (repl *Replica) NewKVStoreWrapper(gid uint64, id int, cluster []string, join bool) {
	log.Infof("peers %v shard id %v", cluster, gid)

	if _, ok := repl.Stores[gid]; ok {
		log.Infof("Shard : %d already present", gid)
		//TODO: To handle config changes
		return
	}

	proposeC := make(chan string)
	//defer close(proposeC)
	confC := make(chan raftpb.ConfChange)
	var confChangeC <-chan raftpb.ConfChange
	confChangeC = confC
	var kvs *kvstore
	getSnapshot := func() ([]byte, error) { return kvs.GetSnapshot() }
	var peerlist []raft.PeerInfo
	for i := range cluster {
		peerlist = append(peerlist, raft.PeerInfo{Id: int(gid)*100 + i + 1, Peer: cluster[i]})
	}

	commitC, errorC, snapshotterReady, rc := raft.NewRaftNode(id, peerlist, join, getSnapshot, proposeC, confChangeC)

	kvs = NewKVStore(<-snapshotterReady, proposeC, commitC, errorC, rc)
	repl.Stores[gid] = kvs
}

func NewKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error, rc *raft.RaftNode) *kvstore {
	s := &kvstore{proposeC: proposeC, KvStore: make(map[string]*value), snapshotter: snapshotter, Node: rc, txnMap: make(map[uint64]Txn)}
	// replay log into key-value map
	s.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go s.readCommits(commitC, errorC)
	return s
}

func (s *kvstore) Put(kv operation) {
	log.Infof("Put value")
	var v value
	v.val = kv.Val
	s.KvStore[kv.Key] = &v
}

// Locks the db
func (s *kvstore) Lock(txn Txn) {

}

/*Txn0: completed 10

Key
Val,  Txn2

WriteIntent : Txn1
Read :Txn2

Txn1: Write     20
Txn2: Read of Txn0

Written:
10 20 30 40

Read:
10 10 20 20 30 30 30 30 40

Read:
10 10 20 20 10 30 20 30 40

Txn0  Txn1 Prep  Txn2Read   Txn1 Commit     Txn3 Read
--------------------------------------------------->
t0    t1          t2           t3             t4
10    10          10           20             20
*/

func getContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	return ctx, cancel
}

// should be called with locks
func (s *kvstore) KvResolveTx(v *value) string {
	log.Infof("KvResolveTx: Resolving the Txn:%+v", v)
	ctx, cancel := getContext()
	defer cancel()

	cli := TxManager.KvTxGetClient()

	txc := &pbt.TxContext{TxId: v.txnId}
	res, err := cli.TxGetRecordState(ctx, &pbt.TxReq{TxContext: txc})
	if err != nil {
		log.Fatalf("TxPrepare: %v", err)
	}

	//v.mu.Lock()
	//defer v.mu.Unlock()
	//XXX: DO we need to update the result in RAFT, we dont need to since its done by
	// commit TXn
	switch res.Stage {
	case "PENDING":
		return "PENDING"
	case "ABORT":
		v.writeIntent = ""
		v.txnId = 0
	case "COMMIT":
		v.val = v.writeIntent
		v.writeIntent = ""
		v.txnId = 0
	}
	return "STAGE"
}

func (s *kvstore) Prep(txn Txn) {

	log.Infof("Got Prep")
	var res int
	res = 1
	//Check for locks
	s.txnPhase = "Prep"
	s.writeIntent = txn.Oper

	for _, oper := range txn.Oper {
		//Should we take lock

		//XXX: Use Read and Write Locks
		var v *value

		// XXX:Can think of readwrite locks
		s.mu.Lock()
		v, is := s.KvStore[oper.Key]
		if !is {
			log.Infof("v is nil")
			v = new(value)
			s.KvStore[oper.Key] = v
		}
		s.mu.Unlock()
		log.Infof("The current Val: %+v for the key:%v", v, oper.Key)
		v.mu.Lock()
		ok := "STAGE"
		if len(v.writeIntent) > 0 {
			ok = s.KvResolveTx(v)
		}
		if ok == "PENDING" {
			res = 0 //abort
			log.Infof("PREPARE ABORTED TxID:%v", txn.TxId)
			v.mu.Unlock()
			break
		}
		res = 1
		v.writeIntent = oper.Val
		v.txnId = txn.TxId
		v.mu.Unlock()
	}
	log.Infof("PREPARED TxID:%v", txn.TxId)
	s.txnMapLock.Lock()
	if stxn, found := s.txnMap[txn.TxId]; found {
		stxn.RespCh <- res
		delete(s.txnMap, txn.TxId)
	}
	s.txnMapLock.Unlock()
}

func (s *kvstore) Commit(txn Txn) {
	s.txnPhase = "Commit"
	for _, oper := range txn.Oper {
		//Should we take a lock
		s.mu.RLock()
		value, ok := s.KvStore[oper.Key]
		s.mu.RUnlock()
		if ok == false {
			log.Infof("The Operation might be completed by other txn")
			return
		}
		value.mu.Lock()
		if value.txnId == txn.TxId {
			value.val = value.writeIntent
			value.writeIntent = ""
		} else {
			log.Infof("COMMIT:Looks like Resolve Tx handled this %v", txn.TxId)
		}
		value.mu.Unlock()
	}
	s.writeIntent = []operation{}
	log.Infof("COMMITED TxID:%v", txn.TxId)

	s.txnMapLock.Lock()
	if stxn, found := s.txnMap[txn.TxId]; found {
		stxn.RespCh <- 1
		delete(s.txnMap, txn.TxId)
	}
	s.txnMapLock.Unlock() //Snapshot
}

func (s *kvstore) Abort(txn Txn) {
	s.txnPhase = "Abort"
	for _, oper := range txn.Oper {
		//Should we take a lokc
		s.mu.RLock()
		value, ok := s.KvStore[oper.Key]
		s.mu.RUnlock()
		if ok == false {
			log.Infof("ABORT: Looks like Tx dint make it to this shard %v", txn.TxId)
			return
		}
		value.mu.Lock()
		if value.txnId == txn.TxId {
			value.val = value.writeIntent
			value.writeIntent = ""
		} else {

			log.Infof("ABORT: Looks like Resolve Tx handled this %v", txn.TxId)
		}
		value.mu.Unlock()
	}
	log.Infof("Aborted TxID:%v", txn.TxId)
	s.writeIntent = []operation{}
	s.txnMapLock.Lock()
	if stxn, found := s.txnMap[txn.TxId]; found {
		stxn.RespCh <- 1
		delete(s.txnMap, txn.TxId)
	}
	s.txnMapLock.Unlock()
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

	s.txnMapLock.Lock()
	s.txnMap[txn.TxId] = txn
	s.txnMapLock.Unlock()
	if err := gob.NewEncoder(&buf).Encode(raftMsg{MsgType: "txn", Rawkv: operation{}, Txn: txn}); err != nil {
		log.Fatal(err)
	}
	log.Infof("propose txn")
	if txn.RespCh != nil {
		log.Infof("RespC is not  nil %v", txn.RespCh)
	}
	s.proposeC <- buf.String()
}

func (s *kvstore) KvHandleTxRead(key string, txId uint64) (KV, error) {
	var kv KV
	log.Infof("Got Tx Read for read:%s", key)
	s.mu.RLock()
	v, ok := s.KvStore[key]
	if ok == false {
		s.mu.RUnlock()
		kv.Key = key
		kv.Val = ""
		log.Infof("Key:%s is not present", key)
		return kv, nil
	}
	s.mu.RUnlock()
	v.mu.Lock()
	//XXX: Raw operation TxId ==0
	if len(v.writeIntent) > 0 {
		if v.txnId < txId {
			_ = s.KvResolveTx(v)

		}
	}
	kv.Val = v.val
	v.mu.Unlock()
	kv.Key = key
	log.Infof("%v", kv)

	return kv, nil
}

func (s *kvstore) HandleKVOperation(key string, val string, op string) (KV, error) {
	var kv KV
	switch op {
	case "GET":
		log.Infof("Got get for key %s val %s", key, val)
		s.mu.RLock()
		v := s.KvStore[key]

		s.mu.RUnlock()
		if v != nil {
			v.mu.Lock()
			if len(v.writeIntent) > 0 {
				s.KvResolveTx(v)
			}
			v.mu.Unlock()
			kv.Val = v.val
		}
		//If v is nil, dont fill kv.Val
		kv.Key = key
	case "PUT":
		fallthrough
	case "POST":
		s.ProposeKV(key, val)
	case "DEL":
		delete(s.KvStore, key)
	}
	return kv, nil
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
			log.Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var msg raftMsg
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		log.Infof("%v", dec)
		if err := dec.Decode(&msg); err != nil {
			log.Fatalf("kvstore: could not decode message (%v)", err)
		}
		//	s.mu.Lock()
		switch msg.MsgType {
		case "txn":
			log.Infof("Got txn from raft")
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
		//	s.mu.Unlock()
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

func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]*value
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.KvStore = store
	return nil
}
func init() {
	//txnMap = make(map[uint64]Txn)
}
