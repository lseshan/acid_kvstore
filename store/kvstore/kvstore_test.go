package kvstore_test

import (
	"context"
	"log"
	"net/http"
	"runtime/debug"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/raft"
	"github.com/acid_kvstore/store/kvstore"
	"go.etcd.io/etcd/raft/raftpb"
)

func Test_kvstore_prep(t *testing.T) {
	var in pb.KvTxWriteReq
	in.TxContext = &pb.TxContext{TxId: 1}
	in.Command = &pb.Command{Op: "GET", Key: 10, Val: 1000}

	resp, err := kvs.KvTxPrepare(context.Background(), &in)
	log.Printf("resp : %v err %v", resp, err)

}

func Test_kvstore_prep_and_commit(t *testing.T) {
	var in pb.KvTxWriteReq
	in.TxContext = &pb.TxContext{TxId: 1}
	in.Command = &pb.Command{Op: "PUT", Key: 10, Val: 1000}

	resp, err := kvs.KvTxPrepare(context.Background(), &in)
	log.Printf("resp : %v err %v", resp, err)

	resp, err = kvs.KvTxCommit(context.Background(), &in)
	log.Printf("resp : %v err %v", resp, err)

}

var kvs *kvstore.Kvstore

func getStackTraceHandler(w http.ResponseWriter, r *http.Request) {
	stack := debug.Stack()
	w.Write(stack)
	pprof.Lookup("goroutine").WriteTo(w, 2)
}

func TestMain(m *testing.M) {
	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)
	cluster := "http://127.0.0.1:10021"
	join := false
	getSnapShot := func() ([]byte, error) { return kvs.GetSnapshot() }
	commitC, errorC, snapshotterReady := raft.NewRaftNode(1, strings.Split(cluster, ","), join, getSnapShot, proposeC, confChangeC)

	kvs = kvstore.NewKVStore(<-snapshotterReady, proposeC, commitC, errorC)
	// get the sum of numbers.
	//http.HandleFunc("/sum", sumConcurrent)
	// get the count of number of go routines in the system.
	//http.HandleFunc("/_count", getGoroutinesCountHandler)
	// respond with the stack trace of the system.
	http.HandleFunc("/_stack", getStackTraceHandler)
	go func() {
		log.Println(http.ListenAndServe("localhost:8001", nil))
	}()
	go kvs.ServeHttpKVApi(9000, errorC)
	m.Run()
	time.Sleep(30 * time.Second)
}
