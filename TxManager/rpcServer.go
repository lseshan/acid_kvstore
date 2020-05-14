package rpcServer
//only start with RAFT KV-STORE LEADER

//should be persistent ?
type rwIntent struct {
	var UUID int //used to know TxRecord state 
	index int 
	var key int
	var val int
	var stage string //Prepare, prepareDone, Commit, commitDone
}

type rwRequest struct {
	id int 
	op string //Read, Write, WriteIntent, Delete
	//wi writeIntent
	key int 
	val int
}

type rwResponse struct {
	id int 
	op string //Read, Write, WriteIntent, Delete
	//wi writeIntent
	key int 
	val int
}

type txRequest struct {
	id int 
	op string //ReadIntent and WriteIntent
	wi writeIntent
	key int 
	val int
}

type txResponse struct {
	id int
	status int //status = 1 success , 0: failure
	op string 
	wi writeIntent
}

/ Handler for a http based key-value s
tore backed by raft
type rpcStruct struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
	isTxOn      int
}

/* 
	Handling TX- asynchronous call
	Opens:
	1. Can the normal read/write happens with ACID TX ?
		- Read (if Tx read, return the staged value)
			- Yes - Non Tx read the non-staged value 
			- No --- > behaviour ?
		- Write (On same TxID the update the staged state)
			- Yes- then cancel the request ?
			- No - Hard to block it till its done
*/
//Prepare and commit
func (r *rpcStruct) procesTxRequest(args *Args, reply *Result) error {
	
}


/*
	1. Read request
	2. Write request 
	3. Delete request
*/
// Read from local key store
func (r *rpcStruct) processRequest(req *rwRequest, rsp *rwResponse) error {
	switch  {
	case req.op == "GET":
		rsp.val, rsp.status = r.store.kvStore[request.key] 
	case req.op == "PUT":

	case req.op == "DELETE":
		//Propose and commit channel
		_, ok := r.store.kvStore[request.key] 
		if (ok) {
			delete(r.store.kvStore, request.key)
		}
	}
	return nil
}

