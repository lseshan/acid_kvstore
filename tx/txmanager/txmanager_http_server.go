package txmanager

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/pprof"
	"net/url"
	"strconv"

	"github.com/gorilla/mux"
)

type TxJson struct {
	TxId string `json:"TxID"`
}

type PostReq struct {
	txid         string
	op, key, val string
}

func (ts *TxStore) handleTxBegin(w http.ResponseWriter, r *http.Request) {

	// XXX: find a way to get the leader
	/*	if ts.RaftNode.IsLeader() {
			log.Printf("Sorry, I am not a leader")
			log.Fatalf("Leader is: XXX")
			return
		}
	*/
	//Creates the Tr with Begin Tx and sends it part of the cookie ?
	tr := NewTxRecord()
	//ts.TxRecordStore[tr.TxId] = tr
	// XXX: ? no need to update with raft as if we fail here dont bother
	ts.TxPending[tr.TxId] = tr
	var res TxJson
	res.TxId = strconv.FormatUint(tr.TxId, 10)
	json.NewEncoder(w).Encode(res)

	//	ts.ProposeTxRecord(*tr)

	log.Printf("Begin: Tx is %d", tr.TxId)

}

func (ts *TxStore) handleTxCommit(w http.ResponseWriter, r *http.Request) {

	//Creates the Tr with Begin Tx and sends it part of the cookie ?
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["txid"], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}

	log.Printf("TxId:%d ", txid)
	tr, ok := ts.TxPending[txid]
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
		return
	}
	res := tr.TxSendBatchRequest()

	if res == true {
		json.NewEncoder(w).Encode("Sucess")
		log.Printf("TxId:%d is Sucess", txid)
	} else {
		json.NewEncoder(w).Encode("Failure")
		log.Printf("TxId:%d is Failure", txid)
	}

}

func (ts *TxStore) handleTxGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["TxId"], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}

	tr, ok := ts.TxRecordStore[txid]
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
	}

	key := vars["key"]
	tr.TxAddCommand(key, "None", "GET")
	json.NewEncoder(w).Encode(tr.TxId)

	log.Printf("Tx GET is %v", key)
}

func (ts *TxStore) handleTxPut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["txid"], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}

	tr, ok := ts.TxRecordStore[txid]
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
	}

	key := vars["key"]
	val := vars["val"]

	tr.TxAddCommand(key, val, "PUT")
	log.Printf("Tx Put is key: %s, val: %s", key, val)
}

func (ts *TxStore) handleTxDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["txid"], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}

	tr, ok := ts.TxRecordStore[txid]
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
	}

	key := vars["key"]
	val := vars["val"]

	tr.TxAddCommand(key, val, "DELETE")
	log.Printf("Tx Delete is key: %s, val: %s", key, val)
}

func (ts *TxStore) handleTxCommand(w http.ResponseWriter, r *http.Request) {
	var tx PostReq
	log.Printf("Handle TxCommand")
	body, _ := ioutil.ReadAll(r.Body)

	log.Printf("%s", body)
	s := string(body)
	m, _ := url.ParseQuery(s)
	//	json.Unmarshal(body, &tx)

	log.Printf("%v", tx)
	txid, err := strconv.ParseUint(m["txid"][0], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}

	key := m["key"][0]
	val := m["val"][0]
	op := m["op"][0]
	log.Printf("http: TxId: %d, key: %s, key: %s, op:%s", txid, key, val, op)
	tr, ok := ts.TxPending[txid]
	if ok == false {
		log.Fatalf("Invalid TxId %v", tx.txid)
	}

	res := tr.TxAddCommand(key, val, op)

	log.Printf("Tx Post is TxId:%d op:%s key: %s, val: %s, Result: %v",
		txid, op, key, val, res)
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
