package kvstore

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/gorilla/mux"
)

func (kvs *Kvstore) handleKVGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["id"]
	var kv KV
	kv = kvs.HandleKVOperation(key, "", "GET")
	json.NewEncoder(w).Encode(kv)

}

func (kvs *Kvstore) handleKVPut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["id"]
	log.Printf("Put is %s", key)
}

func (kvs *Kvstore) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["id"]
	log.Printf("Delete Key is %s", key)
	kvs.HandleKVOperation(key, "", "DEL")
}

func (kvs *Kvstore) handleKVCreate(w http.ResponseWriter, r *http.Request) {
	var kv KV
	body, _ := ioutil.ReadAll(r.Body)
	log.Printf("%s", body)
	json.Unmarshal(body, &kv)
	kvs.HandleKVOperation(kv.Key, kv.Val, "POST")

	log.Printf("Create Key is %s %s", kv.Key, kv.Val)
}

func (kvs *Kvstore) ServeHttpKVApi(port int, errorC <-chan error) {
	r := mux.NewRouter()
	api := r.PathPrefix("/api").Subrouter().StrictSlash(true)

	api.Methods("GET").Subrouter().HandleFunc("/key/{id}", kvs.handleKVGet)
	api.Methods("PUT").Subrouter().HandleFunc("/key/{id}", kvs.handleKVPut)
	api.Methods("DELETE").Subrouter().HandleFunc("/key/{id}", kvs.handleKVDelete)
	api.Methods("POST").Subrouter().HandleFunc("/key", kvs.handleKVCreate)

	//Methods to control Raft

	// Methods to configures KV store - number of shards,

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), r))
}
