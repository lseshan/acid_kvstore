package txmanager

import (
	"encoding/json"
	"io/ioutil"

	//	"log"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strconv"

	log "github.com/pingcap-incubator/tinykv/log"

	pbk "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/gorilla/mux"
)

type TxJson struct {
	TxId    string         `json:"TxID"`
	Status  string         `json:"Status"`
	ReadRsp []*pbk.Command `json:"ReadRsp"`
}

type PostReq struct {
	txid         string
	op, key, val string
}

func (ts *TxStore) handleTxBegin(w http.ResponseWriter, r *http.Request) {

	// XXX: find a way to get the leader
	var res TxJson
	s := ts.RaftNode.GetStatus()
	if ts.RaftNode.IsLeader(s) == false {
		res.Status = "NoLeader"
		json.NewEncoder(w).Encode(res)
		log.Infof("Sorry, I am not a leader")
		log.Infof("Leader is: %v", s.Lead)
		return
	}

	//Creates the Tr with Begin Tx and sends it part of the cookie ?
	tr := NewTxRecord()
	//ts.TxRecordStore[tr.TxId] = tr
	//ts.TxPendingM.Lock()
	//defer ts.TxPendingM.Unlock()
	//ts.TxPending[tr.TxId] = tr
	//XXX:
	/* rt := tr.TxUpdateTxPending("ADD")
	if rt == 0 {
		log.Infof("Error: TxCleanPending failed")
	}
	*/
	res.TxId = strconv.FormatUint(tr.TxId, 10)
	res.Status = "SUCCESS"
	json.NewEncoder(w).Encode(res)

	//	ts.ProposeTxRecord(*tr)

	log.Infof("Begin: Tx is %d", tr.TxId)

}

func (ts *TxStore) handleTxCommit(w http.ResponseWriter, r *http.Request) {

	//Creates the Tr with Begin Tx and sends it part of the cookie ?
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["txid"], 10, 64)
	if err != nil {
		log.Infof("Invalid TxId %v", txid)
	}

	log.Infof("TxId:%d ", txid)
	/* ts.TxPendingM.Lock()
	tr, ok := ts.TxPending[txid]
	ts.TxPendingM.Unlock()
	*/
	/*ts.txPendingLock.Lock()
	tr, ok := ts.TxPending[txid]
	ts.txPendingLock.Unlock()
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
		return
	}
	*/
	ts.mu.RLock()
	tr, ok := ts.TxRecordStore[txid]
	ts.mu.RUnlock()
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
		return
	}

	res := tr.TxSendBatchRequest()

	var ret TxJson
	ret.TxId = strconv.FormatUint(tr.TxId, 10)
	if res == true {
		ret.Status = "SUCCESS"
		if tr.ShardedReadReq != nil {
			for _, val := range tr.ShardedReadReq {
				ret.ReadRsp = append(ret.ReadRsp, val.CommandList...)

			}
		}
		json.NewEncoder(w).Encode(ret)
		log.Infof("Commit Successfull: TxId:%d resp: %+v", txid, ret)
	} else {
		ret.Status = "FAILURE"
		json.NewEncoder(w).Encode(ret)
		log.Infof("TxId:%d resp: %+v is Failure", txid, ret)
	}
}

func (ts *TxStore) handleTxGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["TxId"], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}

	ts.mu.RLock()
	tr, ok := ts.TxRecordStore[txid]
	ts.mu.RUnlock()
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
		return
	}

	key := vars["key"]
	tr.TxAddCommand(key, "None", "GET")
	json.NewEncoder(w).Encode(tr.TxId)

	log.Infof("Tx GET is %v", key)
}

func (ts *TxStore) handleTxPut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["txid"], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}
	ts.mu.RLock()
	tr, ok := ts.TxRecordStore[txid]
	ts.mu.RUnlock()
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
	}

	key := vars["key"]
	val := vars["val"]

	tr.TxAddCommand(key, val, "PUT")
	log.Infof("Tx Put is key: %s, val: %s", key, val)
}

func (ts *TxStore) handleTxDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["txid"], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}

	ts.mu.RLock()
	tr, ok := ts.TxRecordStore[txid]
	ts.mu.RUnlock()
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
		return
	}
	key := vars["key"]
	val := vars["val"]

	tr.TxAddCommand(key, val, "DELETE")
	log.Infof("Tx Delete is key: %s, val: %s", key, val)
}

func (ts *TxStore) handleTxCommand(w http.ResponseWriter, r *http.Request) {
	var tx PostReq
	log.Infof("Handle TxCommand")
	body, _ := ioutil.ReadAll(r.Body)

	log.Infof("%s", body)
	s := string(body)
	m, _ := url.ParseQuery(s)
	//	json.Unmarshal(body, &tx)

	log.Infof("%v", tx)
	log.Infof("%+v", m)
	txid, err := strconv.ParseUint(m["txid"][0], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}
	log.Infof("op:%+v", m["op"])
	var key, val, op string
	op = m["op"][0]
	switch op {
	case "GET":
		key = m["key"][0]
		val = ""
	case "PUT":
		key = m["key"][0]
		val = m["val"][0]
	case "DELETE":
		log.Infof("DELETE Not supported")
		return
	}
	log.Infof("http: TxId: %d, key: %s, key: %s, op:%s", txid, key, val, op)
	//ts.TxPendingM.Lock()
	//ts.txPendingLock.Lock()
	//tr, ok := ts.TxPending[txid]
	//ts.txPendingLock.Unlock()
	//ts.TxPendingM.Unlock()
	//if ok == false {
	//log.Fatalf("Invalid TxId %v", tx.txid)
	//}
	ts.mu.RLock()
	tr, ok := ts.TxRecordStore[txid]
	ts.mu.RUnlock()
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
		return
	}

	res := tr.TxAddCommand(key, val, op)

	log.Infof("Tx Post is TxId:%d op:%s key: %s, val: %s, Result: %v",
		txid, op, key, val, res)
}

func (ts *TxStore) handleTxBatch(w http.ResponseWriter, r *http.Request) {
	var tx PostReq
	log.Infof("Handle TxCommand")
	body, _ := ioutil.ReadAll(r.Body)

	log.Infof("%s", body)
	s := string(body)
	m, _ := url.ParseQuery(s)
	//	json.Unmarshal(body, &tx)

	log.Infof("%v", tx)
	log.Infof("%+v", m)
	tr := NewTxRecord()
	log.Infof("op:%+v", m["op"])
	var key, val string
	for i, op := range m["op"] {
		switch op {
		case "GET":
			key = m["key"][i]
			val = ""
		case "PUT":
			key = m["key"][i]
			val = m["val"][i]
		case "DELETE":
			log.Infof("DELETE Not supported")
			return
		}

		res := tr.TxAddCommand(key, val, op)
		log.Infof("Value received, key: %s, val: %s, op:%s addSuccess: %v", key, val, op, res)
	}

	res := tr.TxSendBatchRequest()

	var ret TxJson
	ret.TxId = strconv.FormatUint(tr.TxId, 10)
	if res == true {
		ret.Status = "SUCCESS"
		if tr.ShardedReadReq != nil {
			for _, val := range tr.ShardedReadReq {
				ret.ReadRsp = append(ret.ReadRsp, val.CommandList...)

			}
		}
		json.NewEncoder(w).Encode(ret)
		log.Infof("Commit Successfull: TxId:%d resp: %+v", tr.TxId, ret)
	} else {
		ret.Status = "FAILURE"
		json.NewEncoder(w).Encode(ret)
		log.Infof("TxId:%d resp: %+v is Failure", tr.TxId, ret)
	}

	//ts.TxPendingM.Lock()
	//ts.txPendingLock.Lock()
	//tr, ok := ts.TxPending[txid]
	//ts.txPendingLock.Unlock()
	//ts.TxPendingM.Unlock()
	//if ok == false {
	//log.Fatalf("Invalid TxId %v", tx.txid)
	//}
}

func (ts *TxStore) handleTxQuery(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["shardinfo"] = ts.ShardInfo
	json.NewEncoder(w).Encode(m)

}

//XXX: Need to verify if errorC is required
func (ts *TxStore) ServeHttpTxApi(port int) {
	r := mux.NewRouter()
	api := r.PathPrefix("/api").Subrouter().StrictSlash(true)

	api.Methods("GET").Subrouter().HandleFunc("/tx/", ts.handleTxBegin)
	api.Methods("GET").Subrouter().HandleFunc("/tx/commit/{txid}/", ts.handleTxCommit)
	api.Methods("GET").Subrouter().HandleFunc("/tx/{txid}/{key}/", ts.handleTxGet)
	api.Methods("PUT").Subrouter().HandleFunc("/tx/{txid}/{key}/{val}", ts.handleTxPut)
	api.Methods("DELETE").Subrouter().HandleFunc("/tx/{txid}/{key}", ts.handleTxDelete)
	api.Methods("POST").Subrouter().HandleFunc("/tx/", ts.handleTxCommand)
	api.Methods("POST").Subrouter().HandleFunc("/tx/batch/", ts.handleTxBatch)

	//Methods to control Raft

	// Methods to configures KV store - number of shards,
	//Debug profile methods
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))

	//Dump internal memory
	api.Methods("GET").Subrouter().HandleFunc("/txmgrquery/", ts.handleTxQuery)

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), r))
}
