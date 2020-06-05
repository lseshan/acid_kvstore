package txmanager_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"testing"

	"github.com/acid_kvstore/tx/txmanager"
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

/*func init() {
	httpport := flag.String("httpport", "23480", "r1:23480, r2:24480, r3:25480")
	flag.Parse()
	log.Printf("%+v", httpport)
	port = *httpport

}
*/
var port = flag.String("port", "23480", "r1:23480, r2:24480, r3:25480")

func TestHttpRequest(t *testing.T) {
	//	httpport := flag.String("httpport", "9121", "r1:23480, r2:24480, r3:25480")
	//	flag.Parse()
	var buffer bytes.Buffer
	buffer.WriteString("http://127.0.0.1:")
	buffer.WriteString(*port)
	buffer.WriteString("/api/tx")

	ul := buffer.String()

	resp, err := http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred")
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	var tx txmanager.TxJson
	json.Unmarshal(body, &tx)

	if tx.Status != "SUCCESS" {
		log.Printf("Test FAILED %s", tx.Status)
		log.Fatalf("Test Failed")
		return
	}
	txid := tx.TxId
	log.Printf("TxId:%s", txid)

	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"PUT"}, "key": {"RJ"}, "val": {"Vmware"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"PUT"}, "key": {"Lakshmi"}, "val": {"Pensada"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"PUT"}, "key": {"Vijaendra"}, "val": {"VMware"}})

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

/*
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
	compl := make(chan int)
	go txmanager.NewTxKvManager(strings.Split(*kvport, ","), compl)
	log.Printf("Waiting to get kvport client")
	<-compl

	//	tr = txmanager.NewTxRecord(cli)
	ts = txmanager.NewTxStore(<-snapshotterReady, proposeC, commitC, errorC, raft)
	go ts.ServeHttpTxApi(kvport, errorC)
	time.Sleep(2 * time.Second)
	os.Exit(m.Run())

}
*/
