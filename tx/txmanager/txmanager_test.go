package txmanager_test

import (
	"bytes"
	"encoding/json"
	"io/ioutil"

	//"log"
	"net/http"
	"net/url"
	"strconv"
	"testing"
	"time"

	log "github.com/pingcap-incubator/tinykv/log"

	"github.com/divan/num2words"

	"github.com/acid_kvstore/tx/txmanager"
)

var netClient = &http.Client{}

/*
var tr *txmanager.TxRecord

func TestTxAddCommand(t *testing.T) {

	rs := tr.TxAddCommand("TOM", "WhoisThis", "PUT")
	log.Infof("result of TxAdd:%t", rs)

	rs = tr.TxAddCommand("Marlo", "This is Me", "PUT")
	log.Infof("result of TxAdd:%t", rs)
}

func TestTxSendBatchRequest(t *testing.T) {
	for _, cm := range tr.CommandList {
		log.Infof("Op:%s", cm.Op)
	}
	rs := tr.TxSendBatchRequest()
	log.Infof("result of TxAdd:%t", rs)

}
*/

/*func init() {
	httpport := flag.String("httpport", "23480", "r1:23480, r2:24480, r3:25480")
	flag.Parse()
	log.Infof("%+v", httpport)
	port = *httpport

}
*/

const replicaURL = "http://127.0.0.1:1026/api/txmanager"

//var num2port map[string]string
func getTxManagerIp() (string, error) {

	resp, err := netClient.Get(replicaURL)
	if err != nil {
		log.Infof("Error Occrred %s", err)
		return "", err
	}
	defer resp.Body.Close()
	//json.Unmarshal(body, &tx)
	//	m := make(map[string]string)
	var m string
	json.NewDecoder(resp.Body).Decode(&m)
	log.Infof("Received Body : %+v Txmanager Tx: %+v", resp.Body, m)
	return m, nil
}

func WriteBatchTxn(key []string, val []string, status chan string) {
	//var buffer bytes.Buffer
	//buffer.WriteString(path)
	//ul := buffer.String()
	/*
			resp, err := netClient.Get(ul)
			if err != nil {
				log.Infof("GET: Error Occrred %s", err)
				status <- "FAILURE"
				return
			}
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		var tx txmanager.TxJson
		json.Unmarshal(body, &tx)

		if tx.Status != "SUCCESS" {
			log.Infof("Test FAILED at GET %s", tx.Status)
			status <- "FAILURE"
			return
		}
	*/
	path := getBatchPath()
	var action []string
	for _, _ = range key {
		action = append(action, "PUT")
	}
	log.Infof("Post: key : %v val: %v", key, val)
	resp, err := http.PostForm(path, url.Values{"op": action, "key": key, "val": val})
	if err != nil {
		log.Infof("GET: Error Occrred %s", err)
		status <- "FAILURE"
		return
	}

	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Infof("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Infof("Test FAILED %+v", res)
		status <- "FAILURE"
		return
	} else {
		log.Infof("Test is Successful %v", res)
		log.Infof("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Infof("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Infof("Received Key:Val: %+v", v)

			}

		}

	}
	/*
		resp1, err := netClient.Get(ul)
		if err != nil {
			log.Infof("Error Occurred %s", err)
			status <- "FAILURE"
			return
		}
		defer resp1.Body.Close()
	*/
	status <- "SUCCESS"

}

func WriteTxn(path string, key []string, val []string, status chan string) {
	var buffer bytes.Buffer
	buffer.WriteString(path)
	ul := buffer.String()

	resp, err := netClient.Get(ul)
	if err != nil {
		log.Infof("GET: Error Occrred %s", err)
		status <- "FAILURE"
		return
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	var tx txmanager.TxJson
	json.Unmarshal(body, &tx)

	if tx.Status != "SUCCESS" {
		log.Infof("Test FAILED at GET %s", tx.Status)
		status <- "FAILURE"
		return
	}
	txid := tx.TxId
	log.Infof("TxId:%s", txid)

	for i := range key {
		log.Infof("Post: key : %s val: %s", key[i], val[i])
		_, _ = http.PostForm(ul, url.Values{"txid": {txid}, "op": {"PUT"}, "key": {key[i]}, "val": {val[i]}})

	}

	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp, err = http.Get(ul)
	if err != nil {
		log.Infof("Error Occurred while Commit, %v", err)
		status <- "FAILURE"
		return
	}
	defer resp.Body.Close()
	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Infof("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Infof("Test FAILED %+v", res)
		status <- "FAILURE"
		return
	} else {
		log.Infof("Test is Successful %v", res)
		log.Infof("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Infof("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Infof("Received Key:Val: %+v", v)

			}

		}

	}
	/*
		resp1, err := netClient.Get(ul)
		if err != nil {
			log.Infof("Error Occurred %s", err)
			status <- "FAILURE"
			return
		}
		defer resp1.Body.Close()
	*/
	status <- "SUCCESS"

}

func ReadBatchTxn(path string, key []string, val []string, status chan string) {
	var buffer bytes.Buffer
	buffer.WriteString(path)
	ul := buffer.String()
	var action []string
	for _, _ = range key {
		action = append(action, "GET")
	}
	log.Infof("Post: key : %v val: %v", key, val)
	resp, err := http.PostForm(ul, url.Values{"op": action, "key": key})
	if err != nil {
		log.Infof("Error Occrred %s", err)
		status <- "FAILURE"
		return
	}

	var res txmanager.TxJson
	json.NewDecoder(resp.Body).Decode(&res)

	if res.Status != "SUCCESS" {
		log.Infof("Test FAILED %+v", res)
		status <- "FAILURE"
		return
	} else {
		log.Infof("Test is Successful %v", res)
		log.Infof("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Infof("Printing Read Results")
			for _, v := range res.ReadRsp {
				log.Infof("Received Key:Val: %+v", v)

			}
		}

	}
	status <- "SUCCESS"
}

func ReadTxn(path string, key []string, val []string, status chan string) {
	var buffer bytes.Buffer
	buffer.WriteString(path)
	ul := buffer.String()

	resp, err := netClient.Get(ul)
	if err != nil {
		log.Infof("Error Occrred %s", err)
		status <- "FAILURE"
		return
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	var tx txmanager.TxJson
	json.Unmarshal(body, &tx)

	if tx.Status != "SUCCESS" {
		log.Infof("Test FAILED %s", tx.Status)
		status <- "FAILURE"
		return
	}
	txid := tx.TxId
	log.Infof("TxId:%s", txid)

	for i := range key {
		log.Infof("Post: key : %s val: %s", key[i], val[i])
		_, _ = http.PostForm(ul, url.Values{"txid": {txid}, "op": {"GET"}, "key": {key[i]}, "val": {val[i]}})

	}

	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp, err = http.Get(ul)
	if err != nil {
		log.Infof("Error Occurred, %v", err)
		status <- "FAILURE"
		return
	}

	var res txmanager.TxJson
	json.NewDecoder(resp.Body).Decode(&res)

	if res.Status != "SUCCESS" {
		log.Infof("Test FAILED %+v", res)
		status <- "FAILURE"
		return
	} else {
		log.Infof("Test is Successful %v", res)
		log.Infof("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Infof("Printing Read Results")
			for _, v := range res.ReadRsp {
				log.Infof("Received Key:Val: %+v", v)

			}
		}

	}
	status <- "SUCCESS"
}

func TestBatchMultipleConcurrentReadTxnMultiOpDifferentScale(t *testing.T) {
	var sucTxn, failTxn int
	value := 3
	status := make(chan string, 1000)
	start := time.Now()
	path := getBatchPath()
	log.Infof(path)
	for i := 1000; i < 2000; i++ {
		time.Sleep(time.Millisecond)
		var key []string
		var val []string
		for j := 0; j < value; j++ {
			key = append(key, strconv.Itoa(i+j*1000))
			val = append(val, num2words.Convert(i+j*1000))
		}
		go func(value int) {
			ReadBatchTxn(path, key, val, status) //[]string{strconv.Itoa(val)}, []string{num2words.Convert(val)}, status)
		}(i)
	}
	for i := 1000; i < 2000; i++ {
		result := <-status
		log.Infof("received %s", result)
		if result == "SUCCESS" {
			sucTxn += 1
		} else if result == "FAILURE" {
			failTxn += 1
		}
	}
	end := time.Since(start)
	log.Warnf("TxnPerSecond %.2f ", float64(sucTxn)/end.Seconds())
	log.Warnf("Succesful txns: %d", sucTxn)
	log.Warnf("Failure txns: %d", failTxn)

}

func TestMultipleConcurrentReadTxnMultiOpDifferentScale(t *testing.T) {
	var sucTxn, failTxn int
	value := 3
	status := make(chan string, 1000)
	start := time.Now()
	path := getPath()
	log.Infof(path)
	for i := 1000; i < 2000; i++ {
		time.Sleep(time.Millisecond)
		var key []string
		var val []string
		for j := 0; j < value; j++ {
			key = append(key, strconv.Itoa(i+j*1000))
			val = append(val, num2words.Convert(i+j*1000))
		}
		go func(value int) {
			ReadTxn(path, key, val, status) //[]string{strconv.Itoa(val)}, []string{num2words.Convert(val)}, status)
		}(i)
	}
	for i := 1000; i < 2000; i++ {
		result := <-status
		log.Infof("received %s", result)
		if result == "SUCCESS" {
			sucTxn += 1
		} else if result == "FAILURE" {
			failTxn += 1
		}
	}
	end := time.Since(start)
	log.Warnf("TxnPerSecond %.2f ", float64(sucTxn)/end.Seconds())
	log.Warnf("Succesful txns: %d", sucTxn)
	log.Warnf("Failure txns: %d", failTxn)

}

func TestBatchMultipleConcurrentWriteTxnMultiOpDifferentScale(t *testing.T) {

	var sucTxn, failTxn int
	value := 3
	status := make(chan string, 1000)
	start := time.Now()
	for i := 1000; i < 2000; i++ {
		time.Sleep(time.Millisecond)
		var key []string
		var val []string
		for j := 0; j < value; j++ {
			key = append(key, strconv.Itoa(i+j*1000))
			val = append(val, num2words.Convert(i+j*1000))
		}
		go func(value int) {
			WriteBatchTxn(key, val, status) //[]string{strconv.Itoa(val)}, []string{num2words.Convert(val)}, status)
		}(i)
	}
	for i := 1000; i < 2000; i++ {
		result := <-status
		log.Infof("received %s", result)
		if result == "SUCCESS" {
			sucTxn += 1
		} else if result == "FAILURE" {
			failTxn += 1
		}
	}
	end := time.Since(start)
	log.Warnf("TxnPerSecond %.2f ", float64(sucTxn)/end.Seconds())
	log.Warnf("Succesful txns: %d", sucTxn)
	log.Warnf("Failure txns: %d", failTxn)

}

func TestMultipleConcurrentWriteTxnMultiOpDifferentScale(t *testing.T) {

	var sucTxn, failTxn int
	value := 3
	status := make(chan string, 1000)
	start := time.Now()
	path := getPath()
	log.Infof(path)
	for i := 1000; i < 2000; i++ {
		time.Sleep(time.Millisecond)
		var key []string
		var val []string
		for j := 0; j < value; j++ {
			key = append(key, strconv.Itoa(i+j*1000))
			val = append(val, num2words.Convert(i+j*1000))
		}
		go func(value int) {
			WriteTxn(path, key, val, status) //[]string{strconv.Itoa(val)}, []string{num2words.Convert(val)}, status)
		}(i)
	}
	for i := 1000; i < 2000; i++ {
		result := <-status
		log.Infof("received %s", result)
		if result == "SUCCESS" {
			sucTxn += 1
		} else if result == "FAILURE" {
			failTxn += 1
		}
	}
	end := time.Since(start)
	log.Warnf("TxnPerSecond %.2f ", float64(sucTxn)/end.Seconds())
	log.Warnf("Succesful txns: %d", sucTxn)
	log.Warnf("Failure txns: %d", failTxn)

}

func TestMultipleConcurrentReadTxnDifferentScale(t *testing.T) {

	var sucTxn, failTxn int
	status := make(chan string, 1000)
	start := time.Now()
	path := getPath()
	for i := 1000; i < 2000; i++ {
		time.Sleep(time.Millisecond)
		go func(val int) {
			ReadTxn(path, []string{strconv.Itoa(val)}, []string{num2words.Convert(val)}, status)
		}(i)
	}
	for i := 1000; i < 2000; i++ {
		result := <-status
		log.Infof("received %s", result)
		if result == "SUCCESS" {
			sucTxn += 1
		} else if result == "FAILURE" {
			failTxn += 1
		}
	}
	end := time.Since(start)
	log.Warnf("TxnPerSecond %.2f ", float64(sucTxn)/end.Seconds())
	log.Warnf("Succesful txns: %d", sucTxn)
	log.Warnf("Failure txns: %d", failTxn)

}

func TestMultipleConcurrentWriteTxnDifferentScale(t *testing.T) {

	var sucTxn, failTxn int
	status := make(chan string, 1000)
	start := time.Now()
	path := getPath()
	for i := 1000; i < 2000; i++ {
		time.Sleep(time.Millisecond)
		go func(val int) {
			WriteTxn(path, []string{strconv.Itoa(val)}, []string{num2words.Convert(val)}, status)
		}(i)
	}
	for i := 1000; i < 2000; i++ {
		result := <-status
		log.Infof("received %s", result)
		if result == "SUCCESS" {
			sucTxn += 1
		} else if result == "FAILURE" {
			failTxn += 1
		}
	}
	end := time.Since(start)
	log.Warnf("TxnPerSecond %.2f ", float64(sucTxn)/end.Seconds())
	log.Warnf("Succesful txns: %d", sucTxn)
	log.Warnf("Failure txns: %d", failTxn)

}

func getBatchPath() string {
	var buffer bytes.Buffer
	for i := 0; i < 6; i++ {
		s, err := getTxManagerIp()
		if err != nil {
			log.Infof("TxManager/ReplicaMgr is not preset")
			time.Sleep(time.Second)
		} else {
			buffer.WriteString(s)
			break
		}
		if i == 5 {
			log.Fatalf("Something has gone wrong")

		}
	}
	buffer.WriteString("/api/tx/batch/")
	return buffer.String()

}

func getPath() string {
	var buffer bytes.Buffer
	for i := 0; i < 6; i++ {
		s, err := getTxManagerIp()
		if err != nil {
			log.Infof("TxManager/ReplicaMgr is not preset")
			time.Sleep(time.Second)
		} else {
			buffer.WriteString(s)
			break
		}
		if i == 5 {
			log.Fatalf("Something has gone wrong")

		}
	}
	buffer.WriteString("/api/tx/")
	return buffer.String()

}
func TestMultipleConcurrentWriteTxnDifferentKey(t *testing.T) {

	status := make(chan string, 10)
	start := time.Now()
	path := getPath()

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
		log.Infof("received %s", result)
	}
	end := time.Since(start)
	log.Warnf("TxnPerSecond %.2f ", end.Seconds()/float64(3))

}

func TestSimpleReadMultiOpTxn(t *testing.T) {
	path := getPath()
	status := make(chan string, 1)

	val := 1
	ReadTxn(path, []string{strconv.Itoa(val), strconv.Itoa(val * 1000)}, []string{num2words.Convert(val), num2words.Convert(val * 1000)}, status)
	result := <-status
	log.Infof("%s", result)

	log.Infof("Done")
}

func TestSimpleBatchWriteMultiOpTxn(t *testing.T) {
	status := make(chan string, 1)

	val := 1
	WriteBatchTxn([]string{strconv.Itoa(val), strconv.Itoa(val * 1000)}, []string{num2words.Convert(val), num2words.Convert(val * 1000)}, status)
	result := <-status
	log.Infof("%s", result)

	log.Infof("Done")
}

func TestSimpleWriteMultiOpTxn(t *testing.T) {
	path := getPath()
	status := make(chan string, 1)

	val := 1
	WriteTxn(path, []string{strconv.Itoa(val), strconv.Itoa(val * 1000)}, []string{num2words.Convert(val), num2words.Convert(val * 1000)}, status)
	result := <-status
	log.Infof("%s", result)

	log.Infof("Done")
}

func TestSimpleWriteTxn(t *testing.T) {
	ul := getPath()

	resp, err := http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred")
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	var tx txmanager.TxJson
	json.Unmarshal(body, &tx)

	if tx.Status != "SUCCESS" {
		log.Infof("Test FAILED %s", tx.Status)
		return
	}
	txid := tx.TxId
	log.Infof("TxId:%s", txid)

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
	resp, err = http.Get(ul)
	if err != nil {
		log.Infof("Error Occurred, %v", err)
	}
	defer resp.Body.Close()
	var res txmanager.TxJson
	json.NewDecoder(resp.Body).Decode(&res)

	if res.Status != "SUCCESS" {
		log.Infof("Test FAILED %+v", res)
		return
	} else {
		log.Infof("Test is Successful %v", res)
		log.Infof("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Infof("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Infof("Received Key:Val: %+v", v)

			}

		}
	}

	log.Infof("Done")
}

func TestSimpleReadWriteTxn(t *testing.T) {
	//	httpport := flag.String("httpport", "9121", "r1:23480, r2:24480, r3:25480")
	//	flag.Parse()
	ul := getPath()

	resp, err := http.Get(ul)
	if err != nil {
		log.Fatalf("Error Occurred")
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	var tx txmanager.TxJson
	json.Unmarshal(body, &tx)

	if tx.Status != "SUCCESS" {
		log.Infof("Test FAILED %s", tx.Status)
		return
	}
	txid := tx.TxId
	log.Infof("TxId:%s", txid)

	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"RJ"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"Lakshmi"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"PUT"}, "key": {"Vijaendra"}, "val": {"VMWARE"}})

	var buffer bytes.Buffer
	buffer.WriteString(ul)
	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp, err = http.Get(ul)
	if err != nil {
		log.Infof("Error Occurred, %v", err)
		return
	}
	defer resp.Body.Close()
	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Infof("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Infof("Test FAILED %+v", res)
		return
	} else {
		log.Infof("Read Test is Successful %v", res)
		log.Infof("Read test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Infof("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Infof("Received Key:Val: %+v", v)

			}

		}

	}

	log.Infof("Done")
}

func TestSimpleReadTxn(t *testing.T) {
	//	httpport := flag.String("httpport", "9121", "r1:23480, r2:24480, r3:25480")
	//	flag.Parse()

	ul := getPath()

	resp, err := http.Get(ul)
	if err != nil {
		log.Warnf("Error Occurred: %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	var tx txmanager.TxJson
	json.Unmarshal(body, &tx)

	if tx.Status != "SUCCESS" {
		log.Infof("Test FAILED %s", tx.Status)
		return
	}
	txid := tx.TxId
	log.Infof("TxId:%s", txid)

	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"RJ"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"Lakshmi"}})
	_, _ = http.PostForm(ul,
		url.Values{"txid": {txid}, "op": {"GET"}, "key": {"Vijaendra"}})

	var buffer bytes.Buffer
	buffer.WriteString(ul)
	buffer.WriteString("commit/")
	buffer.WriteString(txid)
	buffer.WriteString("/")
	ul = buffer.String()
	resp, err = http.Get(ul)
	if err != nil {
		log.Infof("Error Occurred, %v", err)
		return
	}
	defer resp.Body.Close()
	var res txmanager.TxJson
	//json.Unmarshal(body, &tx)
	json.NewDecoder(resp.Body).Decode(&res)

	/*
		body, err = ioutil.ReadAll(resp.Body)
		log.Infof("Http Result %+v", body)
	*/
	if res.Status != "SUCCESS" {
		log.Infof("Test FAILED %+v", res)
		return
	} else {
		log.Infof("Test is Successful %v", res)
		log.Infof("Test is Successful TxID: %s", res.TxId)
		if res.ReadRsp != nil {
			log.Infof("Printing Read Results")

			for _, v := range res.ReadRsp {
				log.Infof("Received Key:Val: %+v", v)

			}

		}

	}

	log.Infof("Done")
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
	log.Infof("Waiting to get kvport client")
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
	//log.SetLevelByString("info")
	log.SetLevelByString("warn")

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)

}
