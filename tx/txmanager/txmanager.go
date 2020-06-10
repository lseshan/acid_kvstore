package txmanager

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"

	//"log"
	"sync"
	"time"

	log "github.com/pingcap-incubator/tinykv/log"

	pbk "github.com/acid_kvstore/proto/package/kvstorepb"
	replpb "github.com/acid_kvstore/proto/package/replicamgrpb"
	"github.com/acid_kvstore/raft"
	"github.com/acid_kvstore/utils"

	//log "github.com/sirupsen/logrus"
	"go.etcd.io/etcd/etcdserver/api/snap"
	raftstat "go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc"
)

/*
func init() {
	log.SetOutput(os.Stdout)
	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)

}

*/
//XXX: modify to increase the rate of abort abd commit
const WorkerBufferLen = 10
const TxTimeout = 10

//XXX: changing back to exported TxStore
var txStore *TxStore

var prevleader uint64

//XXX: After implement TxPending, mantain only state of Commited/Abort in the list
type TxStore struct {
	TxRecordStore map[uint64]*TxRecord
	RaftNode      *raft.RaftNode
	//TxPending     map[uint64]*TxRecord
	//txPendingLock sync.Mutex
	//sddhards
	proposeC         chan<- string // channel for proposing updates
	snapshotter      *snap.Snapshotter
	txnMap           map[uint64]Txn
	txnMapLock       sync.Mutex
	lastTxId         uint64
	HttpEndpoint     string
	RpcEndpoint      string
	ShardInfo        *replpb.ShardInfo
	ReplMgrs         map[string]replpb.ReplicamgrClient
	ReplLeaderClient replpb.ReplicamgrClient
	ReplInfo         *replpb.ReplicaInfo
	mu               sync.RWMutex
	raftStats        raftstat.Status
	commitC          chan TxRecord
	abortC           chan TxRecord
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

type Status struct {
	TxId   uint64
	shard  uint64
	op     string
	status bool
}

type shardServer string

//TxManagerStore[TxId] = { TR, writeIntent }
type TxRecord struct {
	mu sync.Mutex // get the lock variable
	//prOposeC    chan<- string // channel for prOposing writeIntent
	TxId        uint64
	TxStartTime time.Time
	TxPhase     string // PENDING, ABORTED, COMMITED, if PENDING, ABORTED-> switch is OFF else ON
	//txPacket *pbk.KvTxReq
	//w       int    // switch ON/OFF
	//Read to take care off
	CommandList []*pbk.Command
	/// Split between read and writes
	shardedCommands map[uint64][]*pbk.Command
	ShardedReadReq  []*pbk.KvTxReq
	ShardedWriteReq []*pbk.KvTxReq

	//	Wg:          sync.WaitGroup{},
	// Logging the tx, if its crashed ?
	//	TxStore	map[int]TxStore
	//KvClient     *KvClient
	//	Wg           sync.WaitGroup
	/*
		"The record is co-located (i.e. on the same nodes in the distributed system) with the Key in the transAction record."
	*/

}

func init() {
	//warn, error, fatal
	log.SetLevelByString("info")

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}
func NewTxStoreWrapper(id int, cluster []string, join bool) *TxStore {

	log.Infof("peers %v", cluster)

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
	time.Sleep(time.Second)
	return ts
}

/*
func (ts *TxStore) TxCleanPendingList() {
	//call the channel to handle the pending List worker thread
	for _, tr := range ts.TxPending {
		//Push the key to CleanUp worker thread
		if tr.raftTerm != ts.raftStats.Term {
			///XXX: send to channel to service the Cleanup
			res := tr.TxUpdateTxRecord("ABORT")
			if res == 0 {
				log.Infof("Error: TxCleanPending failed")
			}
			log.Infof("RAFT: TxId Abort updated")
			res = tr.TxUpdateTxPending("DEL")
			log.Infof("RAFT: TxId Deleted pending request %d", res)
			ts.abortC <- *tr
		}

	}

}
*/
func doRaftConsensus(tr *TxRecord, s string, wg *sync.WaitGroup) {

	defer wg.Done()
	if tr.TxUpdateTxRecord(s) == 0 {
		log.Infof("ERROR: %s op of TR:%+v is failed", s, tr)
	}
	log.Infof("Changing the state of txid %+v to %s", tr.TxId, s)
	if s == "ABORT" {
		txStore.abortC <- *tr
	}
}

func (ts *TxStore) LogCompactionTimer(ctx context.Context) {

	///XXX: To make sure time is queried
	time.Sleep(TxTimeout * time.Second)

	timeout := time.After(TxTimeout * time.Second)

	for {
		select {
		case <-timeout:
			ts.mu.Lock()
			ts.raftStats = ts.RaftNode.GetStatus()
			ts.mu.Unlock()
			if ts.RaftNode.IsLeader(ts.raftStats) == true {

				var wg sync.WaitGroup
				log.Infof("Querying the pending state")
				ts.mu.RLock()
				for _, tr := range ts.TxRecordStore {
					ts.mu.RUnlock()
					/*if tr.TxPhase == "INIT" {
						ts.mu.RLock()
						continue
					}
					*/
					if tr.TxPhase == "PENDING" || tr.TxPhase == "INIT" {
						elapsedTime := time.Since(tr.TxStartTime)
						// Check if it alright
						if elapsedTime.Seconds() > (float64)(TxTimeout*time.Second) {

							wg.Add(1)
							go doRaftConsensus(tr, "ABORT", &wg)
							//abort after success
							//COMPACTING THE COMMIT TXRecord
						}
					}
					/*	if tr.TxPhase != "ABORT" {
							elapsedTime := time.Since(tr.TxStartTime)
							// Check if it alright
							if elapsedTime.Seconds() > (float64)(TxTimeout*time.Second) {

								if tr.TxPhase == "PENDING" || tr.TxPhase == "INIT" {
									wg.Add(1)
									go doRaftConsensus(tr, "ABORT", &wg)
									//abort after success
									//COMPACTING THE COMMIT TXRecord
								} else if tr.TxPhase == "COMMIT" {
									wg.Add(1)
									go doRaftConsensus(tr, "DELETE", &wg)
								}
							}
						}
					*/
					ts.mu.RLock()
				}
				ts.mu.RUnlock()
				wg.Wait()
			}
			timeout = time.After(TxTimeout * time.Second)
		case <-ctx.Done():
			log.Infof("closed log compaction and abort")
			return

		}
	}
}

func (ts *TxStore) TxCompactCommits() {

	for tr := range ts.commitC {
		for {
			//success then break
			if tr.TxCommit() == true {
				break
			}
			//XXX: Update the leader ctx
			//return only successful
		}
	}

}

func (ts *TxStore) TxCommitWorker() {

	for tr := range ts.commitC {
		var i int
		for i = 0; i < 5; i++ {
			//success then break
			if tr.TxCommit() == true {
				break
			}
			//XXX: Update the leader ctx
			//return only successful
		}
		if i == 5 {
			log.Warnf("ERROR, TxCommit sending is failing")
		}
	}

}

func (ts *TxStore) TxAbortWorker() {
	for tr := range ts.abortC {
		var i int
		for i = 0; i < 5; i++ {
			//success then break
			if tr.TxRollback() == true {
				break
			}
			//XXX: Update the leader ctx
			//return only successful
		}
		if i == 5 {
			log.Warnf("ERROR, TxAbort is failing")
		}

	}
}

func (ts *TxStore) UpdateLeader(ctx context.Context) {
	log.Infof("UpdateTxLeader")
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
			ts.mu.Lock()
			ts.raftStats = ts.RaftNode.GetStatus()
			ts.mu.Unlock()
			if ts.RaftNode.IsLeader(ts.raftStats) {
				_, err := ts.ReplLeaderClient.ReplicaTxLeaderHeartBeat(context.Background(), &out)
				if err != nil {
					log.Infof("error in leader update: %v", err)
				} else {
					log.Infof("sent leader update")
				}
				/*if ts.raftStats.ID != prevleader {
					prevleader = ts.raftStats.ID
					ts.TxCleanPendingList()
				}
				*/

			}
			resp, err := ts.ReplLeaderClient.ReplicaQuery(context.Background(), &replpb.ReplicaQueryReq{})
			if err != nil {
				log.Infof("error in leader update: %v", err)
			} else {
				ts.mu.Lock()
				ts.ShardInfo = resp.ShardInfo
				ts.ReplInfo = resp.ReplicaInfo
				ts.mu.Unlock()
			}
		case <-ctx.Done():
			log.Infof("Done with Update leader")
		}
	}

}

//XXX: may be start multiple threads

func NewTxStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error, r *raft.RaftNode) *TxStore {
	m := make(map[uint64]*TxRecord)
	n := make(map[uint64]Txn)
	rep := make(map[string]replpb.ReplicamgrClient)
	ts := &TxStore{
		TxRecordStore: m,
		proposeC:      proposeC,
		snapshotter:   snapshotter,
		txnMap:        n,
		RaftNode:      r,
		lastTxId:      0,
		ReplMgrs:      rep,
	}
	// replay log into key-value map
	ts.readCommits(commitC, errorC)
	// read commits from raft into kvStore map until error
	go ts.readCommits(commitC, errorC)
	txStore = ts

	//Start commitC and AbortC worker threadsd
	ts.commitC = make(chan TxRecord, WorkerBufferLen)
	ts.abortC = make(chan TxRecord, WorkerBufferLen)
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
	log.Infof("propose txn")

	if tx.RespCh != nil {
		log.Infof("RespC is not  nil %v", tx.RespCh)
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
	txStore = ts
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
			log.Infof("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := ts.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		log.Infof("I am here")
		var msg raftMsg
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		log.Infof("gob result: %+v", dec)
		if err := dec.Decode(&msg); err != nil {
			log.Infof("Tx: could not decode message (%v)", err)
		}
		log.Infof("Update Tx entry: %+v", msg.OpType)
		//XXX: Not sure  we need this
		//	ts.mu.Lock()
		//	defer ts.mu.Unlock()
		tx := msg.Tx
		tr := tx.TxRecord

		// XXX: add failure checks and log message
		switch msg.OpType.RecordType {
		/*	case "TxPending":
			switch msg.OpType.Action {
			case "ADD":
				ts.txPendingLock.Lock()
				//XXX: Would be useful if any node can handles the client
				if tr.TxId > ts.lastTxId {
					ts.lastTxId = tr.TxId
				}
				if _, ok := ts.TxPending[tr.TxId]; ok == true {
					ts.txPendingLock.Unlock()
					//XXX: go to update the error channel
					//log.Infof("Error: ADD Houston we got a problem, Entry:%+v,", r)
					log.Infof("Warning: This entry is created while BEGIN")
					//log.Fatalf("Error: ADD Houston we got a problem, Entry:%+v,", r)
					log.Infof("Warning: This entry is created while BEGIN")
					break
				}
				ts.TxPending[tr.TxId] = tr
				ts.txPendingLock.Unlock()
				log.Infof("Added TxId to TxPending %v", tr.TxId)
			//	log.Infof("Created new Tx entry %+v", tr)
			//Update the state of TR
			case "DEL":
				ts.txPendingLock.Lock()
				if _, ok := ts.TxPending[tr.TxId]; ok == false {
					ts.txPendingLock.Unlock()
					//XXX: go to update the error channel
					log.Infof("Warning: TxPending missing here, TxId:%v", tr.TxId)
					break
				}
				delete(ts.TxPending, tr.TxId)
				ts.txPendingLock.Unlock()
				log.Infof("Deleting from Pending list %v", tr.TxId)
			}
		*/
		// XXX: Might keep track of the state than tr
		case "TxRecordStore":
			switch msg.OpType.Action {
			case "INIT":
				ts.mu.Lock()
				if tr.TxId > ts.lastTxId {
					ts.lastTxId = tr.TxId
				}

				if r, ok := ts.TxRecordStore[tr.TxId]; ok == true {
					ts.mu.Unlock()
					//XXX: Later changed to warning
					log.Infof("Warning:INIT  TxStore entry is already present e:%+v,", r)
					break
				}

				// XXX: This would be redundant for leader , find a way to no-op for leader
				ts.TxRecordStore[tr.TxId] = tr
				tr.TxPhase = "INIT"
				log.Infof("TxID:%v INIT", tr.TxId)

				ts.mu.Unlock()

			case "PENDING":
				ts.mu.RLock()
				tr, ok := ts.TxRecordStore[tr.TxId]
				ts.mu.RUnlock()
				if ok == false {
					//XXX: Later changed to warning
					log.Infof("Warning TxStore  entry is not present e:")
					break
				}

				tr.TxPhase = "PENDING"
				log.Infof("TxID:%v PENDING", tr.TxId)
			/*
				case "COMMIT":
					ts.mu.RLock()
					tr, ok := ts.TxRecordStore[tr.TxId]
					if ok == false {
						//XXX: Later changed to warning
						log.Infof("Warning TxStore  entry is not present e:")
						ts.mu.RUnlock()
						break
					}

					tr.TxPhase = "COMMIT"
					log.Infof("TxID:%v COMMIT", tr.TxId)
					ts.mu.RUnlock()
					log.Infof("TxID:%v COMITED", tr.TxId)
				//	log.Infof("Created new Tx entry %+v", tr)
				//Update the state of TR
			*/
			case "ABORT":
				ts.mu.RLock()
				tr, ok := ts.TxRecordStore[tr.TxId]
				ts.mu.RUnlock()
				if ok == false {
					//XXX: Later changed to warning
					log.Infof("Warning TxStore  entry is not present e:")
					break
				}
				tr.TxPhase = "ABORT"
				log.Infof("TxID:%v ABORTED", tr.TxId)

			case "COMMIT":
				fallthrough
			case "DELETE":
				ts.mu.Lock()
				if _, ok := ts.TxRecordStore[tr.TxId]; ok == false {
					//XXX: Later changed to warning
					log.Warnf("Warning TxStore/Commit shouldnt be here")
					ts.mu.Unlock()
					break
				}
				delete(ts.TxRecordStore, tr.TxId)
				log.Infof("TxID:%v DELETED/COMMIT: TimeStarte:%v", tr.TxId, tr.TxStartTime)
				ts.mu.Unlock()
			}
		}

		ts.txnMapLock.Lock()
		if ltxn, ok := ts.txnMap[tr.TxId]; ok {
			ltxn.RespCh <- 1
		}
		delete(ts.txnMap, tr.TxId)
		ts.txnMapLock.Unlock()
		log.Infof("Raft update done: %+v", msg)

	}
}

func NewTxRecord() *TxRecord {
	tr := &TxRecord{
		TxId:            txStore.getNewTxId(),
		TxPhase:         "INIT",
		CommandList:     make([]*pbk.Command, 0),
		shardedCommands: make(map[uint64][]*pbk.Command),
	}
	tr.TxStartTime = time.Now()
	tr.TxUpdateTxRecord("INIT")
	return tr

}

//verify switch is pending
// Append the write to writeIntent and dispatch the request over prOposechannel
//Does read Operations come here ? NOpe, return the Value there
//func (tr *TxRecord) CreateWriteIntent(Key, Value int ) bool {

func (tr *TxRecord) TxAddCommand(Key, Val, Op string) bool {

	if tr.TxPhase != "INIT" {
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
	txStore.txnMapLock.Lock()
	txStore.txnMap[tr.TxId] = tx
	txStore.txnMapLock.Unlock()
	r := raftType{RecordType: "TxRecordStore", Action: s}
	txStore.ProposeTxRecord(tx, r)
	log.Infof("Done propose of TxStatus, status:%s", s)
	val := <-respCh
	log.Infof("Val %v", val)
	return val
}

// ADD, DEL
/*
func (tr *TxRecord) TxUpdateTxPending(s string) int {

	var tx Txn

	respCh := make(chan int)
	defer close(respCh)

	tx.RespCh = respCh
	tx.TxRecord = tr
	//txStore.mu.Lock()
	txStore.txnMapLock.Lock()
	txStore.txnMap[tr.TxId] = tx
	txStore.txnMapLock.Unlock()
	//txStore.mu.Unlock()
	r := raftType{RecordType: "TxPending", Action: s}
	txStore.ProposeTxRecord(tx, r)
	log.Infof("Done propose of TRPending, status:%s", s)
	val := <-respCh
	log.Infof("Val %v", val)
	return val
}
*/
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
	/* ret := tr.TxUpdateTxPending("ADD")
	log.Infof("TxRAFT: Update Record res:  %v", ret)
	*/

	tr.TxStartTime = time.Now()
	res := tr.TxUpdateTxRecord("PENDING")
	if res == 0 {
		log.Infof("ERROR: PENDING state doesnt seem to be set")

	}
	tr.shardRequest()
	readC := make(chan bool)

	if tr.ShardedReadReq != nil {
		go tr.TxRead(readC)
	}

	if tr.ShardedWriteReq != nil {

		if tr.TxPrepare() == false {
			//dbg failure of the writeIntent
			//invoke the rollback
			res := tr.TxUpdateTxRecord("ABORT")
			log.Infof("RAFT:%v  Abort updated: ret state:%v", tr.TxId, res)
			//res = tr.TxUpdateTxPending("DEL")
			//log.Infof("RAFT:%v Deleted Raft TxPending ret state:%v", tr.TxId, res)
			// XXX: start offloading rollback
			/*	if ok := tr.TxRollback(); ok == true {
					log.Infof("Rollback failed: %v", ok)
				}
			*/
			//XXX: Lets not wait for the read to complete as no need
			// wg.Wait()
			//Send to abort Channel
			txStore.abortC <- *tr
			//tr.TxPhase = "ABORT"
			return false
		}

	}

	if tr.ShardedReadReq != nil {
		state := <-readC
		log.Infof("TxId:%v Read is successfull", tr.TxId)
		if state == false {
			log.Infof("ERROR: Read is Unsuccessful")
			res := tr.TxUpdateTxRecord("ABORT")
			log.Infof("ABORT: RAFT:%v  Abort updated: ret state:%v", tr.TxId, res)
			//res = tr.TxUpdateTxPending("DEL")
			//log.Infof("RAFT:%v Deleted Raft TxPending ret state:%v", tr.TxId, res)
			txStore.abortC <- *tr
			return state
		}

	}
	res = tr.TxUpdateTxRecord("COMMIT")
	log.Infof("Update record: %d", res)

	//	res = tr.TxUpdateTxPending("DEL")
	//log.Infof("Update record: %d", res)

	//Assign commit operation to Commit Channel
	txStore.commitC <- *tr

	close(readC)
	return true
	// XXX: Offload it to worker thread
	/*	if r := tr.TxCommit(); r == false {
		//retry the operation
		// gross error
		//	tr.TxPhase = "ABORT"
		log.Infof("Shouldnt be happening failure in COMMIT")
		/*	if ok := tr.TxRollaback(rq); ok == true {
				log.Infof("Rollback failed: %v", ok)
			}

			res := tr.TxUpdateTxRecord("ABORT")
			log.Infof("Update record: %d", res)
	*/
	/*		return false

	}
	*/

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
/*
func getDummyShardLeader(s uint64) string {

	ctx, cancel := getTxGrpcContext()
	defer cancel()
	resp, err := txStore.ReplLeaderClient.ReplicaQuery(ctx, &replpb.ReplicaQueryReq{})
	if err != nil {
		log.Infof("error in leader update: %v", err)
	}
	txStore.mu.Lock()
	txStore.ShardInfo = resp.ShardInfo
	txStore.mu.Unlock()
	val, ok := txStore.ShardInfo.ShardMap[s]
	if ok == false {
		log.Infof("Missing shard server details: shard: %v", s)
		log.Fatalf("Missing details about shard S")
		return ""
	} else {
		log.Infof("Returning the shard:%v leader details %v", s, val.LeaderKey)
		return val.LeaderKey
		//retrun txStore.ShardInfo.ShardMap[s].getLeaderKey()
	}
}
*/
func getShardLeader(s uint64) string {
	for i := 0; i < 2; i++ {
		txStore.mu.RLock()
		val, ok := txStore.ShardInfo.ShardMap[s]
		txStore.mu.RUnlock()
		if ok == false {
			log.Infof("Missing shard server details: IsQueried:%d, shard: %v", i, s)
			log.Infof("Missing details about shard S")
			ctx, cancel := getTxGrpcContext()
			defer cancel()
			resp, err := txStore.ReplLeaderClient.ReplicaQuery(ctx, &replpb.ReplicaQueryReq{})
			if err != nil {
				log.Infof("error in leader update: %v", err)
			} else {
				txStore.mu.Lock()
				txStore.ShardInfo = resp.ShardInfo
				txStore.mu.Unlock()
			}

		} else {
			log.Infof("Returning the shard:%v leader details %v", s, val.LeaderKey)
			return val.LeaderKey
			//retrun txStore.ShardInfo.ShardMap[s].getLeaderKey()
		}
	}
	return ""
}

// Provide the map of Server with commands
func (tr *TxRecord) shardRequest() {
	readShards := make(map[uint64][]*pbk.Command)
	writeShards := make(map[uint64][]*pbk.Command)
	/// XXX: var cache map[uint64]string
	log.Infof("CommandList:%+v", tr.CommandList)
	for _, cmd := range tr.CommandList {
		//XXX: To DO: Take lock on  txStore. Preferably a read lock
		txStore.mu.RLock()
		nshards := txStore.ReplInfo.Nshards
		txStore.mu.RUnlock()
		log.Infof("Number of shards : %v", nshards)
		shard := utils.Keytoshard(cmd.Key, int(nshards))
		if cmd.Op == "GET" {
			readShards[shard] = append(readShards[shard], cmd)
		} else {
			writeShards[shard] = append(writeShards[shard], cmd)
		}
	}
	tr.setReqPacket(readShards, writeShards)
}

func (tr *TxRecord) setReqPacket(r map[uint64][]*pbk.Command, w map[uint64][]*pbk.Command) {

	for shard, cmdlist := range r {
		cx := new(pbk.TxContext)
		in := new(pbk.KvTxReq)
		in.CommandList = cmdlist
		in.TxContext = cx
		in.TxContext.TxId = tr.TxId
		in.TxContext.ShardId = shard
		tr.ShardedReadReq = append(tr.ShardedReadReq, in)
	}

	for shard, cmdlist := range w {
		cx := new(pbk.TxContext)
		in := new(pbk.KvTxReq)
		in.CommandList = cmdlist
		in.TxContext = cx
		in.TxContext.TxId = tr.TxId
		in.TxContext.ShardId = shard
		tr.ShardedWriteReq = append(tr.ShardedWriteReq, in)
	}

}

func (tr *TxRecord) createSendPacket(shard uint64) *pbk.KvTxReq {
	cx := new(pbk.TxContext)
	in := new(pbk.KvTxReq)
	in.CommandList = tr.shardedCommands[shard]
	in.TxContext = cx
	in.TxContext.TxId = tr.TxId
	in.TxContext.ShardId = shard
	log.Infof("Packet Sent: %+v", in)
	return in
}

//func GetClient()
//staging the change ->kvstore should have logic to abort it if it happens
//sends the prepare message to kvstore raft leader(gRPC/goRPC) and waits for it to finish
//XXX: later we can split depending on shards here

func getTxGrpcContext() (context.Context, context.CancelFunc) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	return ctx, cancel

}

//XXX: we can directly overwrite pointers, just for verifucation for now
func copyReadResults(req *pbk.KvTxReq, resp *pbk.KvTxReply) {
	d := req.CommandList
	s := resp.CommandList

	for i := 0; i < len(d); i++ {
		if d[i].Key == s[i].Key {
			d[i].Val = s[i].Val
		} else {
			log.Infof("Error: missing read results for key: %v, req:%v resp:%v", d[i].Key, req, resp)
		}
	}

}

func (tr *TxRecord) SendGrpcRequest(rq *pbk.KvTxReq, doneC chan Status, op string) {

	var rp *pbk.KvTxReply
	var err error
	var hop int
start:
	server := getShardLeader(rq.TxContext.ShardId)
	//server := getDummyShardLeader(rq.TxContext.ShardId)
	//req := tr.newSendPacket(shard)
	txStore.mu.Lock()
	if _, ok := KvClient[server]; ok == false {
		TxKvCreateClientCtx(server)
		log.Infof("Missed client for server: %s, op:%s", server, op)
	}
	KvGrpc := KvClient[server]
	txStore.mu.Unlock()

	ctx, cancel := getTxGrpcContext()
	defer cancel()

	switch op {
	case "read":
		log.Infof("Tx Read operation")
		rp, err = KvGrpc.Cli.KvTxRead(ctx, rq)

	case "prepare":
		log.Infof("Tx Prepare")
		rp, err = KvGrpc.Cli.KvTxPrepare(ctx, rq)

	case "commit":
		log.Infof("Tx Commit ")
		rp, err = KvGrpc.Cli.KvTxCommit(ctx, rq)

	case "rollback": //abort
		log.Infof("Tx rollback ")
		rp, err = KvGrpc.Cli.KvTxRollback(ctx, rq)
	}
	//XXX: analyze the error later
	if err != nil {
		log.Infof("op:%v, err:%v", op, err)
		//XXX:may be remove on perf study
		if hop == 0 {
			log.Warnf("Looks like shardLeader changed for server:%s", server)
			hop = 1
			txStore.mu.Lock()
			delete(KvClient, server)
			txStore.mu.Unlock()
			goto start
		}
		doneC <- Status{TxId: rq.TxContext.TxId, status: false, op: op, shard: rq.TxContext.ShardId}
		return
	}

	if rp.Status == pbk.Status_Failure {
		doneC <- Status{TxId: rq.TxContext.TxId, status: false, op: op, shard: rq.TxContext.ShardId}
		log.Infof("op:%s failed and rsp: %+v", op, rp)

	} else {
		if op == "read" {
			//copt the result on success
			log.Infof("Read is successfull")
			copyReadResults(rq, rp)

		}
		doneC <- Status{TxId: rq.TxContext.TxId, status: true, op: op, shard: rq.TxContext.ShardId}
		log.Infof("op:%s passed", op)

	}
}

func (tr *TxRecord) TxRead(readC chan bool) {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()

	//tr.shardRequest()
	l := len(tr.ShardedReadReq)
	doneC := make(chan Status, l)
	for _, rq := range tr.ShardedReadReq {
		go tr.SendGrpcRequest(rq, doneC, "read")
	}
	res := true
	for i := 0; i < l; i++ {
		val := <-doneC
		if val.status == false {
			log.Infof("Tx:%s failed for Tx:%+v", val.op, val)
			res = false
		}
	}

	log.Infof("TxRead result: %v", res)
	readC <- res

	close(doneC)
}

func (tr *TxRecord) TxPrepare() bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()

	//tr.shardRequest()
	l := len(tr.ShardedWriteReq)
	doneC := make(chan Status, l)
	for _, rq := range tr.ShardedWriteReq {
		log.Infof("TxPrepare req: %+v", rq)
		go tr.SendGrpcRequest(rq, doneC, "prepare")

	}

	log.Infof("TxPrepare waiting to complete")
	res := true
	for i := 0; i < l; i++ {
		val := <-doneC
		if val.status == false {
			log.Infof("Tx:%s failed for Tx:%+v", val.op, val)
			res = false
		}
	}

	log.Infof("TxResult result: %v", res)
	close(doneC)
	return res
}

func (tr *TxRecord) TxCommit() bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()
	l := len(tr.ShardedWriteReq)
	doneC := make(chan Status, l)

	for _, rq := range tr.ShardedWriteReq {
		log.Infof("TxCommit req: %+v", rq)
		go tr.SendGrpcRequest(rq, doneC, "commit")

	}
	res := true
	for i := 0; i < l; i++ {
		val := <-doneC
		if val.status == false {
			log.Infof("Tx:%s failed for Tx:%+v", val.op, val)
			res = false
		}
	}

	close(doneC)
	log.Infof("TxCommit result: %v", res)
	return res
}

func (tr *TxRecord) TxRollback() bool {
	//send the prepare message to kvstore raft leader (where they Stage the message) using gRPC/goRPC
	//c , err := getClient()
	l := len(tr.ShardedWriteReq)
	doneC := make(chan Status, l)

	for _, rq := range tr.ShardedWriteReq {
		log.Infof("TxRollBack req: %+v", rq)
		go tr.SendGrpcRequest(rq, doneC, "rollback")

	}
	res := true
	for i := 0; i < l; i++ {
		val := <-doneC
		if val.status == false {
			log.Infof("Tx:%s failed for Tx:%+v", val.op, val)
			res = false
		}
	}
	log.Infof("TxRollback result: %v", res)
	close(doneC)
	return res
}

func (ts *TxStore) StartReplicaServerConnection(ctx context.Context, server string) {
	log.Infof("connecting to replmgr: %s", server)
	conn, err := grpc.Dial(server, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Infof("did not connect:%v", err)
	}
	cli := replpb.NewReplicamgrClient(conn)
	ts.ReplMgrs[server] = cli
	log.Infof("connection done")

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
