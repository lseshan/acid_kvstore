package txmanager_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/divan/num2words"

	"github.com/acid_kvstore/tx/txmanager"
)

var netClient = &http.Client{}

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

//var num2port map[string]string

func WriteTxn(path string, key []string, val []string, status chan string) {
	var buffer bytes.Buffer
	buffer.WriteString(path)
	ul := buffer.String()

	resp, err := netClient.Get(ul)
	if err != nil {
		log.Printf("Error Occrred %s", err)
		status <- "FAILURE"
		return
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

	for i := range key {
		log.Printf("Post: key : %s val: %s", key[i], val[i])
		_, _ = http.PostForm(ul, url.Values{"txid": {txid}, "op": {"PUT"}, "key": {key[i]}, "val": {val[i]}})

	}

	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp, err = http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred, %v", err)
	}
	defer resp.Body.Close()
	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Printf("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Printf("Test FAILED %+v", res)
		log.Fatalf("Test Failed")
		status <- "FAILURE"
		return
	} else {
		log.Printf("Test is Successful %v", res)
		log.Printf("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Printf("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Printf("Received Key:Val: %+v", v)

			}

		}

	}
	/*
		resp1, err := netClient.Get(ul)
		if err != nil {
			log.Printf("Error Occurred %s", err)
			status <- "FAILURE"
			return
		}
		defer resp1.Body.Close()
	*/
	status <- "SUCCESS"

}

func ReadTxn(path string, key []string, val []string, status chan string) {
	var buffer bytes.Buffer
	buffer.WriteString(path)
	ul := buffer.String()

	resp, err := netClient.Get(ul)
	if err != nil {
		log.Printf("Error Occrred %s", err)
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

	for i := range key {
		log.Printf("Post: key : %s val: %s", key[i], val[i])
		_, _ = http.PostForm(ul, url.Values{"txid": {txid}, "op": {"GET"}, "key": {key[i]}, "val": {val[i]}})

	}

	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp, err = http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred, %v", err)
	}

	/*
		resp1, err := netClient.Get(ul)
		if err != nil {
			log.Printf("Error Occurred %s", err)
			status <- "FAILURE"
			return
		}
		defer resp1.Body.Close()
	*/
	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Printf("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Printf("Test FAILED %+v", res)
		log.Fatalf("Test Failed")
		status <- "FAILURE"
		return
	} else {
		log.Printf("Test is Successful %v", res)
		log.Printf("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Printf("Printing Read Results")
			for _, v := range res.ReadRsp {
				log.Printf("Received Key:Val: %+v", v)

			}
		}

	}

	status <- "SUCCESS"

}

func TestMultipleConcurrentWriteTxnDifferentScale(t *testing.T) {

	var buffer bytes.Buffer
	var sucTxn, failTxn int
	status := make(chan string, 1000)
	buffer.WriteString("http://127.0.0.1:")
	buffer.WriteString(*port)
	buffer.WriteString("/api/tx/")

	start := time.Now()
	path := buffer.String()
	for i := 1000; i < 2000; i++ {
		go func(val int) {
			WriteTxn(path, []string{strconv.Itoa(val)}, []string{num2words.Convert(val)}, status)
		}(i)
	}
	for i := 1001; i < 2000; i++ {
		result := <-status
		log.Printf("received %s", result)
		if result == "SUCCESS" {
			sucTxn += 1
		} else if result == "FAILURE" {
			failTxn += 1
		}
	}
	end := time.Since(start)
	log.Printf("TxnPerSecond %.2f ", float64(sucTxn)/end.Seconds())
	log.Printf("Succesful txns: %d", sucTxn)
	log.Printf("Failure txns: %d", failTxn)

}
func TestMultipleConcurrentWriteTxnDifferentKey(t *testing.T) {

	var buffer bytes.Buffer
	status := make(chan string, 10)
	buffer.WriteString("http://127.0.0.1:")
	buffer.WriteString(*port)
	buffer.WriteString("/api/tx/")

	start := time.Now()
	path := buffer.String()

	go func() {
		WriteTxn(path, []string{"India"}, []string{"newdelhi"}, status)
	}()
	go func() {
		WriteTxn(path, []string{"USA"}, []string{"DC"}, status)
	}()
	go func() {
		WriteTxn(path, []string{"China"}, []string{"Beijing"}, status)
	}()
	for i := 0; i < 3; i++ {
		result := <-status
		log.Printf("received %s", result)
	}
	end := time.Since(start)
	log.Printf("TxnPerSecond %.2f ", end.Seconds()/float64(3))

}

func TestSimpleWriteTxn(t *testing.T) {
	//	httpport := flag.String("httpport", "9121", "r1:23480, r2:24480, r3:25480")
	//	flag.Parse()
	var buffer bytes.Buffer
	buffer.WriteString("http://127.0.0.1:")
	buffer.WriteString(*port)
	buffer.WriteString("/api/tx/")

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
	resp, err = http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred, %v", err)
	}
	defer resp.Body.Close()
	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Printf("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Printf("Test FAILED %+v", res)
		log.Fatalf("Test Failed")
		return
	} else {
		log.Printf("Test is Successful %v", res)
		log.Printf("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Printf("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Printf("Received Key:Val: %+v", v)

			}

		}

	}

	log.Printf("Done")
}

func TestSimpleReadWriteTxn(t *testing.T) {
	//	httpport := flag.String("httpport", "9121", "r1:23480, r2:24480, r3:25480")
	//	flag.Parse()
	var buffer bytes.Buffer
	buffer.WriteString("http://127.0.0.1:")
	buffer.WriteString(*port)
	buffer.WriteString("/api/tx/")

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
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"RJ"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"Lakshmi"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"PUT"}, "key": {"Vijaendra"}, "val": {"VMWARE"}})

	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp, err = http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred, %v", err)
	}
	defer resp.Body.Close()
	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Printf("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Printf("Test FAILED %+v", res)
		log.Fatalf("Test Failed")
		return
	} else {
		log.Printf("Test is Successful %v", res)
		log.Printf("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Printf("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Printf("Received Key:Val: %+v", v)

			}

		}

	}

	log.Printf("Done")
}

func TestSimpleReadTxn(t *testing.T) {
	//	httpport := flag.String("httpport", "9121", "r1:23480, r2:24480, r3:25480")
	//	flag.Parse()
	var buffer bytes.Buffer
	buffer.WriteString("http://127.0.0.1:")
	buffer.WriteString(*port)
	buffer.WriteString("/api/tx/")

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
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"RJ"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"Lakshmi"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"Vijaendra"}})

	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp, err = http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred, %v", err)
	}
	defer resp.Body.Close()
	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Printf("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Printf("Test FAILED %+v", res)
		log.Fatalf("Test Failed")
		return
	} else {
		log.Printf("Test is Successful %v", res)
		log.Printf("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Printf("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Printf("Received Key:Val: %+v", v)

			}

		}

	}

	log.Printf("Done")
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

func init() {
	tr := &http.Transport{
		MaxIdleConns:        20,
		MaxIdleConnsPerHost: 20,
	}
	netClient = &http.Client{Transport: tr}
}
