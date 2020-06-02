package txmanager

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pbk "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/raft"
	"go.etcd.io/etcd/etcdserver/api/snap"
)

// I am the raft leader
// Start the TxManager/Coordinator
// Create write intent and send the request to the raft to followers- part of prepare
// Once successful/ Send the commit -
//  The followers get the write intent and update the

/*
	1. Each shard has TM running ?
	2.
*/

/*
TransActions are executed in two phases:
https://github.com/cockroachdb/cockroach/blob/master/docs/design.md
Start the transAction by selecting a range which is likely to be heavily involved in the transAction and writing a new transAction record to a reserved area of that range with state "PENDING". In parallel write an "intent" Value for each datum being written as part of the transAction. These are normal MVCC Values,
with the addition of a special flag (i.e. “intent”) indicating that the Value may be committed after the transAction itself commits. In addition,
the transAction id (unique and chosen at txn start time by Client) is stored with intent Values.
The txn id is used to refer to the transAction record when there are conflicts and to make tie-breaking decisions on ordering between identical timestamps.
Each node returns the timestamp used for the write (which is the original candidate timestamp in the absence of read/write conflicts);
the Client selects the maximum from amongst all write timestamps as the final commit timestamp.

Commit the transAction by updating its transAction record. The Value of the commit entry contains the candidate timestamp (increased as necessary to accommodate any latest read timestamps). Note that the transAction is considered fully committed at this point and control may be returned to the Client.

In the case of an SI transAction, a commit timestamp which was increased to accommodate concurrent readers is perfectly acceptable and the commit may continue. For SSI transActions, however, a gap between candidate and commit timestamps necessitates transAction restart (note: restart is different than abort--see below).

After the transAction is committed, all written intents are upgraded in parallel by removing the “intent” flag. The transAction is considered fully committed before this step and does not wait for it to return control to the transac

*/

/* const (
	commit
)
*/
//Keps logs of the record
//kvStore     map[string]kv // current committed Key-Value pairs

/* implement TxRecord Operations */
/*
	Generate TxId
	Create WriteIntent
	Dispatch writeIntent on prOpose channel

*/

/*
type TxRecordStore struct {
	TxRecord *TxRecord
	TxPhase  string // COMMMITED/ABORTED/PENDING //seems to be redundant can even have switch - actually not, TX record represent whole Tx
}
*/

//type TxRecordStore map([uint64]TxStore)
//var TxRecordStore = make(map[uint64]TxStore)

/*
func newTxStore() map[uint64]TxStore {
	TxRecordStore = make(map[uint64]TxStore)
	return TxRecordStore
}
*/

var txStore *TxStore

//XXX: After implement TxPending, mantain only state of Commited/Abort in the list
type TxStore struct {
	TxRecordStore map[uint64]*TxRecord
	RaftNode      *raft.RaftNode
	TxPending     map[uint64]*TxRecord
	//sddhards
	proposeC    chan<- string // channel for proposing updates
	snapshotter *snap.Snapshotter
	txnMap      map[uint64]Txn
	lastTxnId   uint64
	mu          sync.RWMutex
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
	// Logging the tx, if its crashed ?
	//	txStore	map[int]TxStore
	//KvClient     *KvClient
	totalCount   uint64
	prepareCount uint64
	commitCount  uint64
	//	Wg           sync.WaitGroup
	/*
		"The record is co-located (i.e. on the same nodes in the distributed system) with the Key in the transAction record."
	*/

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
	return json.Marshal(ts)
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
			/*if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}*/
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
		TxId:    getTxId,
		TxPhase: "PENDING",
		//KvClient: cli,
		//	Wg:          sync.WaitGroup{},
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
		Idx: tr.totalCount,
		Key: Key,
		Val: Val,
		Op:  Op,
		//Stage: "prepare",
	}

	//We dont need this atomic since SendRequest is serial
	atomic.AddUint64(&tr.totalCount, 1)

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
