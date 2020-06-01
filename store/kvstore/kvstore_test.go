package kvstore_test

import (
	"context"
	"log"
	"net/http"
	"runtime/debug"
	"runtime/pprof"
	"testing"
	"time"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/store/kvstore"
)

func Test_kvstore_prep(t *testing.T) {
	var in pb.KvTxReq
	in.TxContext = &pb.TxContext{TxId: 1}
	localCommand := &pb.Command{Op: "GET", Key: "10", Val: "1000"}
	in.CommandList = append(in.CommandList, localCommand)

	resp, err := kvs.KvTxPrepare(context.Background(), &in)
	log.Printf("resp : %v err %v", resp, err)

}

func Test_kvstore_prep_and_commit(t *testing.T) {
	var in pb.KvTxReq
	in.TxContext = &pb.TxContext{TxId: 1}
	in.CommandList = append(in.CommandList, &pb.Command{Op: "PUT", Key: "10", Val: "1000"})

	resp, err := kvs.KvTxPrepare(context.Background(), &in)
	log.Printf("resp : %v err %v", resp, err)

	resp, err = kvs.KvTxCommit(context.Background(), &in)
	log.Printf("resp : %v err %v", resp, err)

}

func Test_kvreplica_join(t *testing.T) {
	var in pb.ReplicaJoinReq

	in.ShardId = 1
	var shardConfig pb.ShardConfig
	shardConfig.Peers = []string{"https://localhost:9001", "http://localhost:9002"}
	shardConfig.TxLeader = "https://localhost:10001"
	shardConfig.Key = &pb.KeyRange{StartKey: 1, EndKey: 1000}
	in.Config = &shardConfig
	replica.KvReplicaJoin(context.Background(), &in)
	if _, ok := replica.Stores[uint64(in.ShardId)]; ok {
		log.Printf("Replica Join is successuful")
	}

}

func Test_kvreplica_updateconfig(t *testing.T) {
	var in pb.ReplicaConfigReq
	config := &pb.ReplicaConfig{ReplLeader: "https://localhost:9001"}
	in.Config = config
	replica.KvReplicaUpdateConfig(context.Background(), &in)
	log.Printf(replica.Config.ReplLeader)
}

var kvs *kvstore.Kvstore
var replica kvstore.Replica

func getStackTraceHandler(w http.ResponseWriter, r *http.Request) {
	stack := debug.Stack()
	w.Write(stack)
	pprof.Lookup("goroutine").WriteTo(w, 2)
}

func TestMain(m *testing.M) {
	//proposeC := make(chan string)
	//defer close(proposeC)
	//confChangeC := make(chan raftpb.ConfChange)
	//defer close(confChangeC)
	//cluster := "http://127.0.0.1:10021"
	//join := false
	//getSnapShot := func() ([]byte, error) { return kvs.GetSnapshot() }
	//commitC, errorC, snapshotterReady, rc := raft.NewRaftNode(1, strings.Split(cluster, ","), join, getSnapShot, proposeC, confChangeC)

	//kvs = kvstore.NewKVStore(<-snapshotterReady, proposeC, commitC, errorC, rc)
	// get the sum of numbers.
	//http.HandleFunc("/sum", sumConcurrent)
	// get the count of number of go routines in the system.
	//http.HandleFunc("/_count", getGoroutinesCountHandler)
	// respond with the stack trace of the system.
	http.HandleFunc("/_stack", getStackTraceHandler)
	go func() {
		log.Println(http.ListenAndServe("localhost:8001", nil))
	}()
	//go kvs.ServeHttpKVApi(9000, errorC)

	replica.Stores = make(map[uint64]*kvstore.Kvstore)
	replica.ReplicaId = 1

	m.Run()
	time.Sleep(30 * time.Second)

}
