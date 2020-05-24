package txmanager

import (
	"context"
	"log"
	"sync"
	"sync/atomic"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
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
type TxStore struct {
	TxRecord *TxRecord
	TxPhase  string // COMMMITED/ABORTED/PENDING //seems to be redundant can even have switch - actually not, TX record represent whole Tx
}

var TxRecordStore = make(map[uint64]TxStore)

/*
func newTxStore() map[uint64]TxStore {
	TxRecordStore = make(map[uint64]TxStore)
	return TxRecordStore
}
*/

const (
	getUUID = iota
	pi      = 3.14
)

type Command struct {
	//UUID int
	Idx   uint64
	Key   string
	Val   string
	Op    string
	Stage string //prepare, prepareDone, Commit, commitDone
}

type KvClient struct {
	ctx context.Context
	Cli pb.KvstoreClient
}

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
	KvClient     *KvClient
	TotalCount   uint64
	PrepareCount uint64
	CommitCount  uint64
	Wg           sync.WaitGroup
	/*
		"The record is co-located (i.e. on the same nodes in the distributed system) with the Key in the transaction record."
	*/

}

func NewTxRecord(ctx context.Context, Cli pb.KvstoreClient) *TxRecord {
	k := new(KvClient)
	tr := &TxRecord{
		UUID:        getUUID,
		TxPhase:     "PENDING",
		KvClient:    k,
		Wg:          sync.WaitGroup{},
		CommandList: make([]*pb.Command, 0),
	}

	tr.KvClient.ctx = ctx
	tr.KvClient.Cli = Cli

	ts := TxStore{
		TxRecord: tr,
		TxPhase:  "PENDING",
	}
	TxRecordStore[tr.UUID] = ts
	//	tr.txStore = TxRecordStore;
	return tr
	//check for previous store

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
		Idx: tr.TotalCount,
		Key: Key,
		Val: Val,
		Op:  Op,
		//Stage: "prepare",
	}

	//We dont need this atomic since SendRequest is serial
	atomic.AddUint64(&tr.TotalCount, 1)

	tr.CommandList = append(tr.CommandList, rq)
	return true
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

	ts := TxRecordStore[tr.UUID]
	rq := newSendPacket(tr)

	if tr.TxPrepare(rq) == false {
		//dbg failure of the writeIntent
		//invoke the rollback
		ts.TxPhase = "ABORT"
		tr.TxPhase = "ABORT"
		return false
	}

	if tr.TxCommit(rq) == false {
		//retry the operation
		// gross error
		ts.TxPhase = "ABORT"
		tr.TxPhase = "ABORT"
		return false

	}

	log.Printf(" We are good")

	ts.TxPhase = "COMMITED"
	tr.TxPhase = "COMMITED"
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
	rp, err := tr.KvClient.Cli.KvTxPrepare(tr.KvClient.ctx, rq)
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
	rp, err := tr.KvClient.Cli.KvTxCommit(tr.KvClient.ctx, rq)
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
	// do a batch processing
	rp, _ := tr.KvClient.Cli.KvTxRollback(tr.KvClient.ctx, rq)
	//XXX: analyze the error later
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
