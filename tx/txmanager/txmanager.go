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
	replpb "github.com/acid_kvstore/proto/package/replicamgrpb"
	"github.com/acid_kvstore/raft"
	"github.com/acid_kvstore/utils"
	"go.etcd.io/etcd/etcdserver/api/snap"
	raftstat "go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc"
)

//XXX: changing back to exported TxStore
var txStore *TxStore

//XXX: After implement TxPending, mantain only state of Commited/Abort in the list
type TxStore struct {
	TxRecordStore map[uint64]*TxRecord
	RaftNode      *raft.RaftNode
	TxPending     map[uint64]*TxRecord
	//sddhards
	proposeC         chan<- string // channel for proposing updates
	snapshotter      *snap.Snapshotter
	txnMap           map[uint64]Txn
	lastTxId         uint64
	HttpEndpoint     string
	RpcEndpoint      string
	ShardInfo        *replpb.ShardInfo
	ReplMgrs         map[string]replpb.ReplicamgrClient
	ReplLeaderClient replpb.ReplicamgrClient
	mu               sync.Mutex
	raftStats        raftstat.Status
	commitC          chan TxRecord
	abortC           chan TxRecord
	quitC            chan int
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

type shardServer string

//TxManagerStore[TxId] = { TR, writeIntent }
type TxRecord struct {
	mu sync.Mutex // get the lock variable
	//prOposeC    chan<- string // channel for prOposing writeIntent
	TxId     uint64
	TxPhase  string // PENDING, ABORTED, COMMITED, if PENDING, ABORTED-> switch is OFF else ON
	raftTerm uint64
	txPacket *pbk.KvTxReq
	//w       int    // switch ON/OFF
	//Read to take care off
	CommandList []*pbk.Command
	/// Split between read and writes
	shardedCommands map[string][]*pbk.Command
	shardTxResult   map[string]string
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
	InitTxKvMapper()
	return ts
}

func (ts *TxStore) TxCleanPendingList() {
	//call the channel to handle the pending List worker thread
	for _, tr := range ts.TxPending {
		//Push the key to CleanUp worker thread
		if tr.raftTerm != ts.raftStats.Term {
			///XXX: send to channel to service the Cleanup
			res := tr.TxUpdateTxRecord("ABORT")
			if res == 1 {
				log.Fatalf("Error: TxCleanPending failed")
			}
			log.Printf("RAFT: TxId Abort updated")
			res = tr.TxUpdateTxPending("DEL")
			log.Printf("RAFT: TxId Deleted pending request")
			ts.abortC <- *tr
		}

	}

}

var prevleader uint64

func (ts *TxStore) TxCommitWorker() {
	for {
		select {
		case tr := <-ts.commitC:
			for {
				//success then break
				if tr.TxCommit() == false {
					break
				}
				//XXX: Update the leader ctx
				//return only successful
			}
			// No need to update here, as this has to happen

		case <-ts.quitC:
			return

		}
	}
}

func (ts *TxStore) TxAbortWorker() {
	for {
		select {
		case tr := <-ts.abortC:
			for {
				//success then break
				if tr.TxRollback() == false {
					break
				}
				//XXX: Update the leader ctx
				//return only successful
			}
			// No need to update here, as this has to happen

		case <-ts.quitC:
			return
		}
	}
}

func (ts *TxStore) UpdateLeader(ctx context.Context) {
	log.Printf("UpdateTxLeader")
	for {
		select {
		case <-time.After(5 * time.Second):
			if ts.ReplLeaderClient == nil {
				continue
			}
			var out replpb.ReplicaTxReq
			var TxInfo replpb.TxInfo
			TxInfo.HttpEndpoint = ts.HttpEndpoint
			TxInfo.RpcEndpoint = ts.RpcEndpoint
			out.TxInfo = &TxInfo
			ts.raftStats = ts.RaftNode.GetStatus()
			if ts.RaftNode.IsLeader(ts.raftStats) {
				_, err := ts.ReplLeaderClient.ReplicaTxLeaderHeartBeat(context.Background(), &out)
				if err != nil {
					log.Printf("error in leader update: %v", err)
				} else {
					log.Printf("sent leader update")
				}
				if ts.raftStats.ID != prevleader {
					prevleader = ts.raftStats.ID
					ts.TxCleanPendingList()
				}

			}
			resp, err := ts.ReplLeaderClient.ReplicaQuery(context.Background(), &replpb.ReplicaQueryReq{})
			if err != nil {
				log.Printf("error in leader update: %v", err)
			} else {
				ts.mu.Lock()
				ts.ShardInfo = resp.ShardInfo
				ts.mu.Unlock()
			}

		case <-ctx.Done():
			log.Printf("Done with Update leader")
		}
	}

}

//XXX: may be start multiple threads
const WorkerBufferLen = 10

func NewTxStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error, r *raft.RaftNode) *TxStore {
	m := make(map[uint64]*TxRecord)
	q := make(map[uint64]*TxRecord)
	n := make(map[uint64]Txn)
	rep := make(map[string]replpb.ReplicamgrClient)
	ts := &TxStore{
		TxRecordStore: m,
		proposeC:      proposeC,
		snapshotter:   snapshotter,
		txnMap:        n,
		RaftNode:      r,
		TxPending:     q,
		lastTxId:      0,
		ReplMgrs:      rep,
	}
	// replay log into key-value map
	ts.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go ts.readCommits(commitC, errorC)
	txStore = ts

	//Start commitC and AbortC worker threadsd
	ts.raftStats = r.GetStatus()
	ts.commitC = make(chan TxRecord, WorkerBufferLen)
	ts.abortC = make(chan TxRecord, WorkerBufferLen)
	ts.quitC = make(chan int, 2)
	return ts
}

func (ts *TxStore) getNewTxId() uint64 {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.lastTxId = ts.lastTxId + 1
	return ts.lastTxId
}

//XXX: Is this right snapshot
func (ts *TxStore) GetSnapshot() ([]byte, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return json.Marshal(ts.TxRecordStore)
	//XXX: Understand impact of both
	//return json.Marshal(ts)
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
	//XXX
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
				ts.mu.Lock()
				//XXX: Would be useful if any node can handles the client
				if tr.TxId > ts.lastTxId {
					ts.lastTxId = tr.TxId
				}
				if _, ok := ts.TxPending[tr.TxId]; ok == true {
					ts.mu.Unlock()
					//XXX: go to update the error channel
					//log.Fatalf("Error: ADD Houston we got a problem, Entry:%+v,", r)
					log.Printf("Warning: This entry is created while BEGIN")
					break
				}
				ts.TxPending[tr.TxId] = tr
				ts.mu.Unlock()
				log.Printf("Added TxId to TxPending %v", tr.TxId)
			//	log.Printf("Created new Tx entry %+v", tr)
			//Update the state of TR
			case "DEL":
				ts.mu.Lock()
				if _, ok := ts.TxPending[tr.TxId]; ok == false {
					ts.mu.Unlock()
					//XXX: go to update the error channel
					log.Fatalf("Warning: TxPending missing here, TxId:%v", tr.TxId)
					break
				}
				delete(ts.TxPending, tr.TxId)
				ts.mu.Unlock()
				log.Printf("Deleting from Pending list %v", tr.TxId)
			}
		// XXX: Might keep track of the state than tr
		case "TxRecordStore":
			switch msg.OpType.Action {
			case "NEW":
				fallthrough
			case "COMMIT":
				ts.mu.Lock()
				if r, ok := ts.TxRecordStore[tr.TxId]; ok == true {
					ts.mu.Unlock()
					//XXX: Later changed to warning
					log.Fatalf("Warning TxStore/Commit entry is already present e:%+v,", r)
				}

				// XXX: This would be redundant for leader , find a way to no-op for leader
				ts.TxRecordStore[tr.TxId] = tr
				tr.TxPhase = "COMMIT"
				log.Printf("TxID:%v COMITED", tr.TxId)

				ts.mu.Unlock()
			//	log.Printf("Created new Tx entry %+v", tr)
			//Update the state of TR
			case "ABORT":
				ts.mu.Lock()
				if r, ok := ts.TxRecordStore[tr.TxId]; ok == true {
					//XXX: Later changed to warning
					log.Fatalf("Warning TxStore/Commit entry is already present e:%+v,", r)
					ts.mu.Unlock()
					break
				}

				ts.TxRecordStore[tr.TxId] = tr
				tr.TxPhase = "ABORT"
				log.Printf("TxID:%v ABORTED", tr.TxId)
				ts.mu.Unlock()
			}
		}

		if ltxn, ok := ts.txnMap[tr.TxId]; ok {
			ltxn.RespCh <- 1
			delete(ts.txnMap, tr.TxId)
		}
		log.Printf("Raft update done: %+v", msg)

	}
}

func NewTxRecord() *TxRecord {
	tr := &TxRecord{
		TxId:            txStore.getNewTxId(),
		TxPhase:         "PENDING",
		CommandList:     make([]*pbk.Command, 0),
		shardedCommands: make(map[string][]*pbk.Command),
		shardTxResult:   make(map[string]string),
	}
	tr.raftTerm = txStore.raftStats.Term
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
	ret := tr.TxUpdateTxPending("ADD")
	log.Printf("TxRAFT: Update Record res:  %v", ret)

	tr.shardRequest()
	if tr.TxPrepare() == false {
		//dbg failure of the writeIntent
		//invoke the rollback
		/* res := tr.TxUpdateTxRecord("ABORT")
		log.Printf("RAFT: TxId Abort updated")
		res = tr.TxUpdateTxPending("DEL")
		log.Printf("RAFT: TxId Deleted pending request")
		// XXX: start offloading rollback
		if ok := tr.TxRollback(rq); ok == true {
			log.Printf("Rollback failed: %v", ok)
		}
		*/
		//Send to abort Channel
		txStore.abortC <- *tr
		//tr.TxPhase = "ABORT"
		return false
	}

	res := tr.TxUpdateTxRecord("COMMIT")
	res = tr.TxUpdateTxPending("DEL")

	//Assign commit operation to Commit Channel
	txStore.commitC <- *tr

	// XXX: Offload it to worker thread
	// http://nesv.github.io/golang/2014/02/25/worker-queues-in-go.html
	/*	if r := tr.TxCommit(); r == false {
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
	/*		return false

	}
	*/
	log.Printf(" We are good")

	log.Printf("Update record: %d", res)
	//tr.TxPhase = "COMMITED"
	return true
}

//func newSednPacket(tr *TxRecord) [string]*pbk.KvTxReq
//{
//Wallk all TxRecord for each command:
//   command
//   shard :=  key2shard (each keys in commandList)
//   Leader: TxManager.ShardMap(shard).GetLeader()
//   KvTxReq[Leader] = command
//}

//For each leader (3)
//Send KvTxReq

func getShardLeader(s uint64) string {
	txStore.mu.Lock()
	defer txStore.mu.Unlock()

	if val, ok := txStore.ShardInfo.ShardMap[s]; ok == false {
		log.Fatalf("looks like we have a failure here")
		log.Fatalf("Missing details about shard S")
		return ""
	} else {
		return val.LeaderKey
		//retrun txStore.ShardInfo.ShardMap[s].getLeaderKey()
	}
}

// Provide the map of Server with commands
func (tr *TxRecord) shardRequest() {

	/// XXX: var cache map[uint64]string
	for _, cmd := range tr.CommandList {
		shard := utils.Keytoshard(cmd.Key, 3)
		s := getShardLeader(shard)
		tr.shardedCommands[s] = append(tr.shardedCommands[s], cmd)
	}

}

func (tr *TxRecord) newSendPacket(s string) *pbk.KvTxReq {
	cx := new(pbk.TxContext)
	in := new(pbk.KvTxReq)
	in.CommandList = tr.shardedCommands[s]
	in.TxContext = cx
	in.TxContext.TxId = tr.TxId
	return in

}

//func GetClient()
//staging the change ->kvstore should have logic to abort it if it happens
//sends the prepare message to kvstore raft leader(gRPC/goRPC) and waits for it to finish
//XXX: later we can split depending on shards here

func getTxGrpcContext() (context.Context, context.CancelFunc) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	return ctx, cancel

}

func (tr *TxRecord) SendGrpcRequest(s string, doneC chan bool, op string) {

	ctx, cancel := getTxGrpcContext()
	defer cancel()

	var rp *pbk.KvTxReply
	var err error
	req := tr.newSendPacket(s)
	if _, ok := KvClient[s]; ok == false {
		TxKvCreateClientCtx(s)
	}
	switch op {

	case "prepare":
		rp, err = KvClient[s].Cli.KvTxPrepare(ctx, req)

	case "commit":
		rp, err = KvClient[s].Cli.KvTxCommit(ctx, req)

	case "rollback":
		rp, err = KvClient[s].Cli.KvTxRollback(ctx, req)
	}
	//XXX: analyze the error later
	if err != nil {
		log.Fatalf("op:%v, err:%v", op, err)
		doneC <- false
	}

	if rp.Status == pbk.Status_Failure {
		tr.shardTxResult[s] = "FAIL"
		doneC <- false
		log.Printf("op:%s failed", op)

	} else {
		tr.shardTxResult[s] = "PASS"
		doneC <- true
		log.Printf("op:%s failed", op)

	}
}

func (tr *TxRecord) TxPrepare() bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()

	tr.shardRequest()
	l := len(tr.shardedCommands)
	doneC := make(chan bool, l)
	for server, _ := range tr.shardedCommands {
		go tr.SendGrpcRequest(server, doneC, "prepare")

	}
	res := true
	for i := 0; i < l; i++ {
		val := <-doneC
		if val == false {
			res = false
		}
	}
	return res
}

func (tr *TxRecord) TxCommit() bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()
	l := len(tr.shardedCommands)
	doneC := make(chan bool, l)

	for server, _ := range tr.shardedCommands {
		go tr.SendGrpcRequest(server, doneC, "commit")

	}
	res := true
	for i := 0; i < l; i++ {
		val := <-doneC
		if val == false {
			res = false
		}
	}
	return res
}

func (tr *TxRecord) TxRollback() bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()
	l := len(tr.shardedCommands)
	doneC := make(chan bool, l)

	for server, _ := range tr.shardedCommands {
		go tr.SendGrpcRequest(server, doneC, "rollback")

	}
	res := true
	for i := 0; i < l; i++ {
		val := <-doneC
		if val == false {
			res = false
		}
	}
	log.Fatalf("TxRollback failed")
	return res
}

func (ts *TxStore) StartReplicaServerConnection(ctx context.Context, server string) {
	log.Printf("connecting to replmgr: %s", server)
	conn, err := grpc.Dial(server, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect:%v", err)
	}
	cli := replpb.NewReplicamgrClient(conn)
	ts.ReplMgrs[server] = cli
	log.Printf("connection done")

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
