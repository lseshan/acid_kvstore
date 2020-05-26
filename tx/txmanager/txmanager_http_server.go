package txmanager

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
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

	//Creates the Tr with Begin Tx and sends it part of the cookie ?
	tr := NewTxRecord(ts.KvClient)
	ts.TxRecordStore[tr.UUID] = tr
	var res TxJson
	res.TxId = strconv.FormatUint(tr.UUID, 10)
	json.NewEncoder(w).Encode(res)

	//	ts.ProposeTxRecord(*tr)

	log.Printf("Begin: Tx is %d", tr.UUID)

}

func (ts *TxStore) handleTxCommit(w http.ResponseWriter, r *http.Request) {

	//Creates the Tr with Begin Tx and sends it part of the cookie ?
	vars := mux.Vars(r)
	txid, err := strconv.ParseUint(vars["txid"], 10, 64)
	if err != nil {
		log.Fatalf("Invalid TxId %v", txid)
	}

	log.Printf("TxId:%d ", txid)
	tr, ok := ts.TxRecordStore[txid]
	if ok == false {
		log.Fatalf("Invalid TxId %v", txid)
		return
	}

	ret := tr.TxUpdateTxRecord("New")
	log.Printf("Update Record res:  %v", ret)

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
	json.NewEncoder(w).Encode(tr.UUID)

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
	log.Printf("I am here")
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
	log.Printf("TxId: %d, key: %s, key: %s, op:%s", txid, key, val, op)
	tr, ok := ts.TxRecordStore[txid]
	if ok == false {
		log.Fatalf("Invalid TxId %v", tx.txid)
	}

	res := tr.TxAddCommand(key, val, op)

	log.Printf("Tx Post is TxId:%d op:%s key: %s, val: %s, Result: %v",
		txid, op, key, val, res)
}

func (ts *TxStore) ServeHttpTxApi(port int, errorC <-chan error) {
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

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), r))
}
