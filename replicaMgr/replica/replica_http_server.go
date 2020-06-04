package replicamgr

import (
	"encoding/json"
	"log"
	"net/http"
	"net/http/pprof"
	"strconv"

	"github.com/gorilla/mux"
)

func (repl *ReplicaMgr) handleReplicaMgrGet(w http.ResponseWriter, r *http.Request) {
	m := make(map[string]interface{})
	m["Shard"] = repl.Shard
	m["txmgr"] = repl.TxInfo
	json.NewEncoder(w).Encode(m)
}

func (repl *ReplicaMgr) ServeHttpReplicamgrApi(port int) {
	log.Printf("Server replicamgr")
	r := mux.NewRouter()
	api := r.PathPrefix("/api").Subrouter().StrictSlash(true)
	api.Methods("GET").Subrouter().HandleFunc("/replica", repl.handleReplicaMgrGet)

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
