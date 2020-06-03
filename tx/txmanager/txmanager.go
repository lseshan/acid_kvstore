package txmanager

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"
	"time"

	pbk "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/raft"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
)

//XXX: changing back to exported TxStore
var txStore *TxStore

//XXX: After implement TxPending, mantain only state of Commited/Abort in the list
type TxStore struct {
	TxRecordStore map[uint64]*TxRecord
	RaftNode      *raft.RaftNode
	TxPending     map[uint64]*TxRecord
	proposeC      chan<- string // channel for proposing updates
	snapshotter   *snap.Snapshotter
	txnMap        map[uint64]Txn
	lastTxnId     uint64
	mu            sync.RWMutex
}

type Txn struct {
	TxRecord *TxRecord
	RespCh   chan<- int
}

type raftType struct {
	RecordType string
	Action     string // : New, UpdateCommands, UpdateStatui
}

type raftMsg struct {
	Tx     Txn
	OpType raftType
}

const (
	getTxId = iota + 100
)

//TxManagerStore[TxId] = { TR, writeIntent }
type TxRecord struct {
	mu sync.RWMutex // get the lock variable
	//prOposeC    chan<- string // channel for prOposing writeIntent
	TxId    uint64
	TxPhase string // PENDING, ABORTED, COMMITED, if PENDING, ABORTED-> switch is OFF else ON
	//w       int    // switch ON/OFF
	//Read to take care off
	CommandList []*pbk.Command
	//	Wg:          sync.WaitGroup{},
	// Logging the tx, if its crashed ?
	//	TxStore	map[int]TxStore
	//KvClient     *KvClient
	//	Wg           sync.WaitGroup
	/*
		"The record is co-located (i.e. on the same nodes in the distributed system) with the Key in the transAction record."
	*/

}

func NewTxStoreWrapper(id int, cluster []string, join bool) *TxStore {

	log.Printf("peers %v", cluster)

	proposeC := make(chan string)
	//defer close(proposeC)
	confC := make(chan raftpb.ConfChange)
	var confChangeC <-chan raftpb.ConfChange
	confChangeC = confC
	var ts *TxStore
	getSnapshot := func() ([]byte, error) { return ts.GetSnapshot() }
	var peerlist []raft.PeerInfo
	for i := range cluster {
		peerlist = append(peerlist, raft.PeerInfo{Id: i + 1, Peer: cluster[i]})
	}

	commitC, errorC, snapshotterReady, rc := raft.NewRaftNode(id, peerlist, join, getSnapshot, proposeC, confChangeC)

	ts = NewTxStore(<-snapshotterReady, proposeC, commitC, errorC, rc)
	return ts
}

func NewTxStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error, r *raft.RaftNode) *TxStore {
	m := make(map[uint64]*TxRecord)
	q := make(map[uint64]*TxRecord)
	n := make(map[uint64]Txn)
	ts := &TxStore{
		TxRecordStore: m,
		proposeC:      proposeC,
		snapshotter:   snapshotter,
		txnMap:        n,
		RaftNode:      r,
		TxPending:     q,
	}
	// replay log into key-value map
	ts.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go ts.readCommits(commitC, errorC)
	txStore = ts
	return ts
}

//XXX: Is this right snapshot
func (ts *TxStore) GetSnapshot() ([]byte, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return json.Marshal(ts.TxRecordStore)
}

func (ts *TxStore) ProposeTxRecord(tx Txn, t raftType) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(raftMsg{Tx: tx, OpType: t}); err != nil {
		log.Fatal(err)
	}
	log.Printf("propose txn")

	if tx.RespCh != nil {
		log.Printf("RespC is not  nil %v", tx.RespCh)
	}
	ts.proposeC <- buf.String()
}

func (ts *TxStore) recoverFromSnapshot(snapshot []byte) error {
	var TxRecords map[uint64]*TxRecord
	if err := json.Unmarshal(snapshot, &TxRecords); err != nil {
		return err
	}
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.TxRecordStore = TxRecords
	return nil
}

func (ts *TxStore) readCommits(commitC <-chan *string, errorC <-chan error) {

	for data := range commitC {
		if data == nil {
			// done replaying log; new data incoming
			// OR signaled to load snapshot
			snapshot, err := ts.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := ts.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		log.Printf("I am here")
		var msg raftMsg
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		log.Printf("gob result: %+v", dec)
		if err := dec.Decode(&msg); err != nil {
			log.Fatalf("Tx: could not decode message (%v)", err)
		}
		log.Printf("Update Tx entry: %+v", msg.OpType)
		//XXX: Not sure  we need this
		//	ts.mu.Lock()
		//	defer ts.mu.Unlock()
		tx := msg.Tx
		tr := tx.TxRecord

		// XXX: add failure checks and log message
		switch msg.OpType.RecordType {
		case "TxPending":
			switch msg.OpType.Action {
			case "ADD":
				if _, ok := ts.TxPending[tr.TxId]; ok == true {
					//XXX: go to update the error channel
					//log.Fatalf("Error: ADD Houston we got a problem, Entry:%+v,", r)
					log.Printf("Warning: This entry is created while BEGIN")
					//	break
				}
				ts.TxPending[tr.TxId] = tr
				log.Printf("Added TxId to TxPending %v", tr.TxId)
			//	log.Printf("Created new Tx entry %+v", tr)
			//Update the state of TR
			case "DEL":
				if _, ok := ts.TxPending[tr.TxId]; ok == false {
					//XXX: go to update the error channel
					log.Fatalf("Error: DEL Houston we got a problem, TxId:%v", tr.TxId)
					break
				}
				delete(ts.TxPending, tr.TxId)
				log.Printf("Deleting the ")
			}
		// XXX: Might keep track of the state than tr
		case "TxRecordStore":
			switch msg.OpType.Action {
			case "NEW":
				fallthrough
			case "COMMIT":
				if r, ok := ts.TxRecordStore[tr.TxId]; ok == true {
					//XXX: Later changed to warning
					log.Fatalf("Warning TxStore/Commit entry is already present e:%+v,", r)
					break
				}

				// XXX: This would be redundant for leader , find a way to no-op for leader
				ts.TxRecordStore[tr.TxId] = tr
				tr.TxPhase = "COMMIT"
				log.Printf("TxID:%v COMITED", tr.TxId)

			//	log.Printf("Created new Tx entry %+v", tr)
			//Update the state of TR
			case "ABORT":
				if r, ok := ts.TxRecordStore[tr.TxId]; ok == true {
					//XXX: Later changed to warning
					log.Fatalf("Warning TxStore/Commit entry is already present e:%+v,", r)
					break
				}

				ts.TxRecordStore[tr.TxId] = tr
				tr.TxPhase = "ABORT"
				log.Printf("TxID:%v ABORTED", tr.TxId)
			}
		}

		if ltxn, ok := ts.txnMap[tr.TxId]; ok {
			ltxn.RespCh <- 1
		}
		delete(ts.txnMap, tr.TxId)
		log.Printf("Raft update done: %+v", msg)

	}
}

func NewTxRecord() *TxRecord {
	tr := &TxRecord{
		TxId:        getTxId,
		TxPhase:     "PENDING",
		CommandList: make([]*pbk.Command, 0),
	}
	return tr

}

//verify switch is pending
// Append the write to writeIntent and dispatch the request over prOposechannel
//Does read Operations come here ? NOpe, return the Value there
//func (tr *TxRecord) CreateWriteIntent(Key, Value int ) bool {

func (tr *TxRecord) TxAddCommand(Key, Val, Op string) bool {

	if tr.TxPhase != "PENDING" {
		return false
	}

	rq := &pbk.Command{
		Idx: 0, //XXX: delete this field
		Key: Key,
		Val: Val,
		Op:  Op,
		//Stage: "prepare",
	}

	tr.CommandList = append(tr.CommandList, rq)
	return true
}

//COMMIT, ABORT
func (tr *TxRecord) TxUpdateTxRecord(s string) int {

	var tx Txn

	respCh := make(chan int)
	defer close(respCh)

	tx.RespCh = respCh
	tx.TxRecord = tr
	txStore.txnMap[tr.TxId] = tx
	r := raftType{RecordType: "TxRecordStore", Action: s}
	txStore.ProposeTxRecord(tx, r)
	log.Printf("Done propose of TxStatus, status:%s", s)
	val := <-respCh
	log.Printf("Val %v", val)
	return val
}

// ADD, DEL
func (tr *TxRecord) TxUpdateTxPending(s string) int {

	var tx Txn

	respCh := make(chan int)
	defer close(respCh)

	tx.RespCh = respCh
	tx.TxRecord = tr
	txStore.txnMap[tr.TxId] = tx
	r := raftType{RecordType: "TxPending", Action: s}
	txStore.ProposeTxRecord(tx, r)
	log.Printf("Done propose of TRPending, status:%s", s)
	val := <-respCh
	log.Printf("Val %v", val)
	return val
}

func (tr *TxRecord) TxSendBatchRequest() bool {
	/* 	tr.mu.Lock()
	   	defer tr.mu.Unlock()
	   	if tr.TxPhase == "ABORT" {
	   		//	tr.mu.Unlock()
	   		tr.Wg.Done()
	   		return false
	   	}
	   	//	tr.mu.RUnLock()
	*/

	rq := newSendPacket(tr)

	if tr.TxPrepare(rq) == false {
		//dbg failure of the writeIntent
		//invoke the rollback
		res := tr.TxUpdateTxRecord("ABORT")
		log.Printf("RAFT: TxId Abort updated")
		res = tr.TxUpdateTxPending("DEL")
		log.Printf("RAFT: TxId Deleted pending request")
		// XXX: start offloading rollback
		if ok := tr.TxRollaback(rq); ok == true {
			log.Printf("Rollback failed: %v", ok)
		}

		log.Printf("Update record: %d", res)
		//tr.TxPhase = "ABORT"
		return false
	}

	res := tr.TxUpdateTxRecord("COMMIT")
	res = tr.TxUpdateTxPending("DEL")
	// XXX: Offload it to worker thread
	// http://nesv.github.io/golang/2014/02/25/worker-queues-in-go.html
	if r := tr.TxCommit(rq); r == false {
		//retry the operation
		// gross error
		//	tr.TxPhase = "ABORT"
		log.Fatalf("Shouldnt be happening failure in COMMIT")
		/*	if ok := tr.TxRollaback(rq); ok == true {
				log.Printf("Rollback failed: %v", ok)
			}

			res := tr.TxUpdateTxRecord("ABORT")
			log.Printf("Update record: %d", res)
		*/
		return false

	}

	log.Printf(" We are good")

	log.Printf("Update record: %d", res)
	//tr.TxPhase = "COMMITED"
	return true
}

func newSendPacket(tr *TxRecord) *pbk.KvTxReq {
	cx := new(pbk.TxContext)

	in := new(pbk.KvTxReq)
	in.CommandList = tr.CommandList
	in.TxContext = cx
	in.TxContext.TxId = tr.TxId
	return in

}

//func GetClient()
//staging the change ->kvstore should have logic to abort it if it happens
//sends the prepare message to kvstore raft leader(gRPC/goRPC) and waits for it to finish
//XXX: later we can split depending on shards here
func (tr *TxRecord) TxPrepare(rq *pbk.KvTxReq) bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rp, err := KvClient.Cli.KvTxPrepare(ctx, rq)
	//XXX: analyze the error later
	if err != nil {
		log.Fatalf("TxPrepare: %v", err)
	}
	if rp.Status == 0 {
		log.Printf("TxPrepare succeeded")
		return true
	}
	log.Fatalf("TxPrepare failed")
	return false
}

func (tr *TxRecord) TxCommit(rq *pbk.KvTxReq) bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	rp, err := KvClient.Cli.KvTxCommit(ctx, rq)
	if err != nil {
		log.Fatalf("TxCommit: %v", err)
	}
	//XXX: analyze the error later
	if rp.Status == 0 {
		log.Printf("TxCommit succeeded")
		return true
	}

	log.Fatalf("TxCommit failed")
	return false
}

//Rollback sync: batch the request
func (tr *TxRecord) TxRollaback(rq *pbk.KvTxReq) bool {
	//canceliing the request
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	// do a batch processing
	rp, err := KvClient.Cli.KvTxRollback(ctx, rq)
	//XXX: analyze the error later
	if err != nil {
		log.Fatalf("TxRollback: %v", err)
	}

	if rp.Status == 0 {
		log.Printf("Rollback succeeded")
		return true
	}
	log.Fatalf("TxRollback failed")
	return false

	//Fail the request--> Move state to FAILED to ABORT and unStage all
}

// Reading a Value -> ask from leaseholder ?

//GenerateTxId ->relies on iota

/*
	Verify if you can send the Key with writeIntent if its alright current node since its replicated node
	if its sharded node, then we need to send the prepare message(with chances of abort to all the nodes)
*/

//SendPrepare

//SendCommit

//TimeOut for abort
//Time
//abort the Tx with TxId

// goRPC:HTTP:$PORT each leader
//Leader / Service Discon
//How to find TxManager Leader and KV-store Leader ?
