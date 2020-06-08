package kvstore

import (
	"encoding/json"
	"io/ioutil"

	//"log"
	"net/http"
	"net/http/pprof"
	"strconv"

	log "github.com/pingcap-incubator/tinykv/log"

	"github.com/acid_kvstore/utils"
	"github.com/gorilla/mux"
)

func (repl *Replica) handleKVGet(w http.ResponseWriter, r *http.Request) {
	//Get the kvstore-for now use 0
	vars := mux.Vars(r)
	key := vars["id"]
	shard := utils.Keytoshard(key, int(repl.Config.Nshards))
	kvs := repl.Stores[shard]
	var kv KV
	kv, _ = kvs.HandleKVOperation(key, "", "GET")
	json.NewEncoder(w).Encode(kv)
}

func (kvs *Kvstore) handleKVGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["id"]
	var kv KV
	kv, _ = kvs.HandleKVOperation(key, "", "GET")
	json.NewEncoder(w).Encode(kv)

}

func (repl *Replica) handleKVPut(w http.ResponseWriter, r *http.Request) {
	//kvs := repl.Stores[0]
	vars := mux.Vars(r)
	key := vars["id"]
	log.Infof("Put is %s", key)
}

func (kvs *Kvstore) handleKVPut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["id"]
	log.Infof("Put is %s", key)
}

func (repl *Replica) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	kvs := repl.Stores[0]
	vars := mux.Vars(r)
	key := vars["id"]
	log.Infof("Delete Key is %s", key)
	kvs.HandleKVOperation(key, "", "DEL")
}

func (kvs *Kvstore) handleKVDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["id"]
	log.Infof("Delete Key is %s", key)
	kvs.HandleKVOperation(key, "", "DEL")
}

func (repl *Replica) handleKVCreate(w http.ResponseWriter, r *http.Request) {
	kvs := repl.Stores[0]
	var kv KV
	body, _ := ioutil.ReadAll(r.Body)
	log.Infof("%s", body)
	json.Unmarshal(body, &kv)
	kvs.HandleKVOperation(kv.Key, kv.Val, "POST")

	log.Infof("Create Key is %s %s", kv.Key, kv.Val)
}

func (kvs *Kvstore) handleKVCreate(w http.ResponseWriter, r *http.Request) {
	var kv KV
	body, _ := ioutil.ReadAll(r.Body)
	log.Infof("%s", body)
	json.Unmarshal(body, &kv)
	kvs.HandleKVOperation(kv.Key, kv.Val, "POST")

	log.Infof("Create Key is %s %s", kv.Key, kv.Val)
}

func (repl *Replica) handleReplicaConfigGet(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["ReplicaInfo"] = repl.Config
	json.NewEncoder(w).Encode(m)

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
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), r))
}

func (repl *Replica) ServeHttpReplicaApi(port int) {
	r := mux.NewRouter()
	api := r.PathPrefix("/api").Subrouter().StrictSlash(true)
	api.Methods("GET").Subrouter().HandleFunc("/replicaconfig/", repl.handleReplicaConfigGet)

	api.Methods("GET").Subrouter().HandleFunc("/key/{id}", repl.handleKVGet)
	api.Methods("PUT").Subrouter().HandleFunc("/key/{id}", repl.handleKVPut)
	api.Methods("DELETE").Subrouter().HandleFunc("/key/{id}", repl.handleKVDelete)
	api.Methods("POST").Subrouter().HandleFunc("/key", repl.handleKVCreate)

	//Methods to control Raft

	// Methods to configures KV store - number of shards,
	r.HandleFunc("/debug/pprof/", pprof.Index)
	r.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	r.HandleFunc("/debug/pprof/profile", pprof.Profile)
	r.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	r.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	r.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	r.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	r.Handle("/debug/pprof/block", pprof.Handler("block"))

	log.Fatal(http.ListenAndServe(":"+strconv.Itoa(port), r))
}
