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

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
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
Transactions are executed in two phases:
https://github.com/cockroachdb/cockroach/blob/master/docs/design.md
Start the transaction by selecting a range which is likely to be heavily involved in the transaction and writing a new transaction record to a reserved area of that range with state "PENDING". In parallel write an "intent" Value for each datum being written as part of the transaction. These are normal MVCC Values,
with the addition of a special flag (i.e. “intent”) indicating that the Value may be committed after the transaction itself commits. In addition,
the transaction id (unique and chosen at txn start time by Client) is stored with intent Values.
The txn id is used to refer to the transaction record when there are conflicts and to make tie-breaking decisions on ordering between identical timestamps.
Each node returns the timestamp used for the write (which is the original candidate timestamp in the absence of read/write conflicts);
the Client selects the maximum from amongst all write timestamps as the final commit timestamp.

Commit the transaction by updating its transaction record. The Value of the commit entry contains the candidate timestamp (increased as necessary to accommodate any latest read timestamps). Note that the transaction is considered fully committed at this point and control may be returned to the Client.

In the case of an SI transaction, a commit timestamp which was increased to accommodate concurrent readers is perfectly acceptable and the commit may continue. For SSI transactions, however, a gap between candidate and commit timestamps necessitates transaction restart (note: restart is different than abort--see below).

After the transaction is committed, all written intents are upgraded in parallel by removing the “intent” flag. The transaction is considered fully committed before this step and does not wait for it to return control to the transac

*/

/* const (
	commit
)
*/
//Keps logs of the record
//kvStore     map[string]kv // current committed Key-Value pairs

/* implement TxRecord Operations */
/*
	Generate UUID
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

type TxStore struct {
	TxRecordStore map[uint64]*TxRecord
	KvClient      *KvClient
	RaftNode      *raft.RaftNode

	//sddhards
	proposeC    chan<- string // channel for proposing updates
	snapshotter *snap.Snapshotter
	txnMap      map[uint64]Txn
	mu          sync.RWMutex
}

type Txn struct {
	TxRecord *TxRecord
	RespCh   chan<- int
}

type raftMsg struct {
	Tx     Txn
	OpType string // Status : New, UpdateCommands, UpdateStatus
}

const (
	getUUID = iota + 100
)

//XXX: We can maintain map of this if we are doing shards??
type KvClient struct {
	Cli pb.KvstoreClient
}

var kvStorecli *KvClient

//TxManagerStore[UUID] = { TR, writeIntent }
type TxRecord struct {
	mu sync.RWMutex // get the lock variable
	//prOposeC    chan<- string // channel for prOposing writeIntent
	UUID    uint64
	TxPhase string // PENDING, ABORTED, COMMITED, if PENDING, ABORTED-> switch is OFF else ON
	//w       int    // switch ON/OFF
	//Read to take care off
	CommandList []*pb.Command
	// Logging the tx, if its crashed ?
	//	txStore	map[int]TxStore
	//KvClient     *KvClient
	totalCount   uint64
	prepareCount uint64
	commitCount  uint64
	//	Wg           sync.WaitGroup
	/*
		"The record is co-located (i.e. on the same nodes in the distributed system) with the Key in the transaction record."
	*/

}

func NewTxStore(cl *KvClient, snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error, r *raft.RaftNode) *TxStore {
	m := make(map[uint64]*TxRecord)
	n := make(map[uint64]Txn)
	ts := &TxStore{
		TxRecordStore: m,
		KvClient:      cl,
		proposeC:      proposeC,
		snapshotter:   snapshotter,
		txnMap:        n,
		RaftNode:      r,
	}
	// replay log into key-value map
	ts.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go ts.readCommits(commitC, errorC)
	txStore = ts
	return ts
}

func (ts *TxStore) GetSnapshot() ([]byte, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return json.Marshal(ts.TxRecordStore)
}

func (ts *TxStore) ProposeTxRecord(tx Txn, op string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(raftMsg{Tx: tx, OpType: op}); err != nil {
		log.Fatal(err)
	}
	log.Printf("propose txn")

	if tx.RespCh != nil {
		log.Printf("RespC is not  nil %v", tx.RespCh)
	}
	ts.proposeC <- buf.String()
}

func (ts *TxStore) readCommits(commitC <-chan *string, errorC <-chan error) {

	log.Printf("I am in Read Commit")
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

		var msg raftMsg
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		log.Printf("gob result: %v", dec)
		if err := dec.Decode(&msg); err != nil {
			log.Fatalf("Tx: could not decode message (%v)", err)
		}
		log.Printf("Update Tx entry: %v", msg.OpType)
		//XXX: Not sure  we need this
		//	ts.mu.Lock()
		//	defer ts.mu.Unlock()
		tx := msg.Tx
		tr := tx.TxRecord

		// XXX: add failure checks and log message
		switch msg.OpType {
		case "New":
			// XXX: This would be redundant for leader , find a way to no-op for leader
			ts.TxRecordStore[tr.UUID] = tr

		//	log.Printf("Created new Tx entry %+v", tr)
		//Update the state of TR
		case "COMMITED":
			fallthrough
		case "ABORT":
			if r, ok := ts.TxRecordStore[tr.UUID]; ok == false {
				//XXX: go to update the error channel
				log.Fatalf("Error finding the record store")
				r.TxPhase = "ABORT"
			} else {
				r.TxPhase = msg.OpType
			}
			log.Printf("Update Tx entry: %v", msg.OpType)
		}
		if ltxn, ok := ts.txnMap[tr.UUID]; ok {
			ltxn.RespCh <- 1
		}
		delete(ts.txnMap, tr.UUID)
		log.Printf("Confirmation should have got")
	}
}

func NewTxRecord(cli *KvClient) *TxRecord {
	tr := &TxRecord{
		UUID:    getUUID,
		TxPhase: "PENDING",
		//KvClient: cli,
		//	Wg:          sync.WaitGroup{},
		CommandList: make([]*pb.Command, 0),
	}
	kvStorecli = cli
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

	rq := &pb.Command{
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

func (tr *TxRecord) TxUpdateTxRecord(s string) int {

	var tx Txn

	respCh := make(chan int)
	defer close(respCh)

	tx.RespCh = respCh
	tx.TxRecord = tr
	txStore.txnMap[tr.UUID] = tx
	txStore.ProposeTxRecord(tx, s)
	log.Printf("Done propose of TR, status:%s", s)
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
		if ok := tr.TxRollaback(rq); ok == true {
			log.Printf("Rollback failed: %v", ok)
		}
		res := tr.TxUpdateTxRecord("ABORT")
		log.Printf("Update record: %d", res)
		//tr.TxPhase = "ABORT"
		return false
	}

	if tr.TxCommit(rq) == false {
		//retry the operation
		// gross error
		//	tr.TxPhase = "ABORT"
		if ok := tr.TxRollaback(rq); ok == true {
			log.Printf("Rollback failed: %v", ok)
		}

		res := tr.TxUpdateTxRecord("ABORT")
		log.Printf("Update record: %d", res)
		return false

	}

	log.Printf(" We are good")

	res := tr.TxUpdateTxRecord("COMMITED")
	log.Printf("Update record: %d", res)
	//tr.TxPhase = "COMMITED"
	return true
}

func newSendPacket(tr *TxRecord) *pb.KvTxReq {
	cx := new(pb.TxContext)

	in := new(pb.KvTxReq)
	in.CommandList = tr.CommandList
	in.TxContext = cx
	in.TxContext.TxId = tr.UUID
	return in

}

//func GetClient()
//staging the change ->kvstore should have logic to abort it if it happens
//sends the prepare message to kvstore raft leader(gRPC/goRPC) and waits for it to finish
//XXX: later we can split depending on shards here
func (tr *TxRecord) TxPrepare(rq *pb.KvTxReq) bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	rp, err := kvStorecli.Cli.KvTxPrepare(ctx, rq)
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

func (tr *TxRecord) TxCommit(rq *pb.KvTxReq) bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	rp, err := kvStorecli.Cli.KvTxCommit(ctx, rq)
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
func (tr *TxRecord) TxRollaback(rq *pb.KvTxReq) bool {
	//canceliing the request
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	// do a batch processing
	rp, err := kvStorecli.Cli.KvTxRollback(ctx, rq)
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

//GenerateUUID ->relies on iota

/*
	Verify if you can send the Key with writeIntent if its alright current node since its replicated node
	if its sharded node, then we need to send the prepare message(with chances of abort to all the nodes)
*/

//SendPrepare

//SendCommit

//TimeOut for abort
//Time
//abort the Tx with UUID

// goRPC:HTTP:$PORT each leader
//Leader / Service Discon
//How to find TxManager Leader and KV-store Leader ?
