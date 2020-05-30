package txmanager_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/raft"
	"github.com/acid_kvstore/tx/txmanager"
	"go.etcd.io/etcd/raft/raftpb"
	"google.golang.org/grpc"
)

/*
var tr *txmanager.TxRecord

func TestTxAddCommand(t *testing.T) {

	rs := tr.TxAddCommand("TOM", "WhoisThis", "PUT")
	log.Printf("result of TxAdd:%t", rs)

	rs = tr.TxAddCommand("Marlo", "This is Me", "PUT")
	log.Printf("result of TxAdd:%t", rs)
}

func TestTxSendBatchRequest(t *testing.T) {
	for _, cm := range tr.CommandList {
		log.Printf("Op:%s", cm.Op)
	}
	rs := tr.TxSendBatchRequest()
	log.Printf("result of TxAdd:%t", rs)

}
*/

func TestHttpRequest(t *testing.T) {

	ul := "http://127.0.0.1:50055/api/tx/"
	resp, err := http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred")
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	var tx txmanager.TxJson
	json.Unmarshal(body, &tx)

	txid := tx.TxId
	log.Printf("TxId:%s", txid)

	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"PUT"}, "key": {"RJ"}, "val": {"Vmware"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"PUT"}, "key": {"Lakshmi"}, "val": {"Pensada"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"PUT"}, "key": {"Vijaendra"}, "val": {"VMware"}})

	var buffer bytes.Buffer
	buffer.WriteString(ul)
	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp1, err := http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred")
	}
	defer resp1.Body.Close()
	log.Printf("What?")
}

const (
	address     = "localhost:50051"
	defaultName = "world"
)

func TestMain(m *testing.M) {
	// call flag parser if needed
	kvport := 50055
	cluster := "http://127.0.0.1:25555"
	join := false
	id := 1

	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("grpc connection failed")
	}

	defer conn.Close()

	c := pb.NewKvstoreClient(conn)

	proposeC := make(chan string)
	defer close(proposeC)

	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var ts *txmanager.TxStore
	getSnapshot := func() ([]byte, error) { return ts.GetSnapshot() }
	commitC, errorC, snapshotterReady, raft := raft.NewRaftNode(id, strings.Split(cluster, ","), join, getSnapshot, proposeC, confChangeC)
	cli := &txmanager.KvClient{c}
	//	tr = txmanager.NewTxRecord(cli)
	ts = txmanager.NewTxStore(cli, <-snapshotterReady, proposeC, commitC, errorC, raft)
	go ts.ServeHttpTxApi(kvport, errorC)
	time.Sleep(2 * time.Second)
	os.Exit(m.Run())

}
