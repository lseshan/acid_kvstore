package TxManager
// I am the raft leader 
// Start the TxManager/Coordinator 
// Create write intent and send the request to the raft to followers- part of prepare 
// Once successful/ Send the commit - 
//  The followers get the write intent and update the 

/*
	1. Each shard has TM running ?
	2. 
*/

/* 
Transactions are executed in two phases:
https://github.com/cockroachdb/cockroach/blob/master/docs/design.md
Start the transaction by selecting a range which is likely to be heavily involved in the transaction and writing a new transaction record to a reserved area of that range with state "PENDING". In parallel write an "intent" value for each datum being written as part of the transaction. These are normal MVCC values, 
with the addition of a special flag (i.e. “intent”) indicating that the value may be committed after the transaction itself commits. In addition, 
the transaction id (unique and chosen at txn start time by client) is stored with intent values. 
The txn id is used to refer to the transaction record when there are conflicts and to make tie-breaking decisions on ordering between identical timestamps. 
Each node returns the timestamp used for the write (which is the original candidate timestamp in the absence of read/write conflicts); 
the client selects the maximum from amongst all write timestamps as the final commit timestamp.

Commit the transaction by updating its transaction record. The value of the commit entry contains the candidate timestamp (increased as necessary to accommodate any latest read timestamps). Note that the transaction is considered fully committed at this point and control may be returned to the client.

In the case of an SI transaction, a commit timestamp which was increased to accommodate concurrent readers is perfectly acceptable and the commit may continue. For SSI transactions, however, a gap between candidate and commit timestamps necessitates transaction restart (note: restart is different than abort--see below).

After the transaction is committed, all written intents are upgraded in parallel by removing the “intent” flag. The transaction is considered fully committed before this step and does not wait for it to return control to the transac

*/

const (
	commit
)

//Keps logs of the record 
type TxStore struct {
	var Tx *TxRecord
	var switch string // COMMMITED/ABORTED/PENDING //seems to be redundant can even have switch - actually not, TX record represent whole Tx
}
//kvStore     map[string]kv // current committed key-value pairs 

/* implement TxRecord operations */
/*
	Generate UUID
	Create WriteIntent
	Dispatch writeIntent on propose channel 

*/
//TxManagerStore[UUID] = { TR, writeIntent }

func newTxStore() map[int]TxStore {
	ts := make(map[int]TxStore) 
	return ts
}

const (

	genUUID = iota
	pi = 3.14
)
//should be persistent ?
type writeIntent struct {
	var UUID int //used to know TxRecord state 
	var key int
	var val int
	var stage string //Prepare, prepareDone, Commit, commitDone
}

TxManagerStore[UUID] = { TR, writeIntent }
type TxRecord struct {
	var mu sync.RWMutex // get the lock variable 
	//proposeC    chan<- string // channel for proposing writeIntent
	UUID int 
	TxPhase string // PENDING, ABORTED, COMMITED, if PENDING, ABORTED-> switch is OFF else ON 
	var switch int // switch ON/OFF
	//Read to take care off
	WriteList []writeIntent  
						// Logging the tx, if its crashed ?
	txStore	map[int]TxStore

	
	/*
	"The record is co-located (i.e. on the same nodes in the distributed system) with the key in the transaction record."
	*/

}

func newTxRecord(TxRecordStore map[int]TxStore) *TxRecord {
	tr := &TxRecord{
			UUID = genUUID,
			TxPhase = "PENDING",
			switch = 0,
	}
	TxRecordStore[tr.UUID].Tx = tr
	TxRecordStore[tr.UUID].switch = false // switch is off
	return tr
	//check for previous store  

}

func (tr *TxRecord) UpdateTxStore(txStore map[int]TxStore ) {
	tr.txStore = txStore;
	//send the prepare message to kvstore raft leader (where they stage the message) using gRPC/goRPC
}

//verify switch is pending 
// Append the write to writeIntent and dispatch the request over proposechannel
//Does read operations come here ? Nope, return the value there
func (tr *TxRecord) CreateWriteIntent(key, value int ) bool {
	tr.mu.RLock()
	if(tr.TxPhase != "PENDING") {
		tr.mu.RUnLock()
		return false
	}
	tr.mu.RUnLock()

	wr := writeIntent{
			index : len(wr)
			UUID: tr.UUID,
			key : key,
			val : val,
		}
	tr.WriteList = append(tr.WriteList, wr)
	if (!tr.prepare(wr)) {
		//dbg failure of the writeIntent
		return false
	}

	tr.propose(wr)
	return true 
}

//staging the change ->kvstore should have logic to abort it if it happens
//sends the prepare message to kvstore raft leader(gRPC/goRPC) and waits for it to finish
//XXX: later we can split depending on shards here
func (tr *TxRecord) prepare(wr WriteIntent) {
	wr.stage = prepare
	//send the prepare message to kvstore raft leader (where they stage the message) using gRPC/goRPC
}

//All the staged change complete 

// Callback when prepare is complete/ 
// when prepare is complete -> all the data is been staged
//UUID to query the tr 
//wrapper over prepare complete to route to particular TX record
func (tr *TxRecord) PrepareComplete(index int) {
	//on sucess
	if (tr.WriteList[index].stage = prepare) {
		tr.WriteList[index].stage = prepareDone
	}
}	

//flip the switch
func (tr *TxRecord) Commit(index int) {
	//flip the switch 
	if (tr.WriteList[index].stage = prepareDone) {
		tr.WriteList[index].stage = Commit
	}
}

// Callback when prepare is complete/ 
func (tr *TxRecord) CommitComplete(index int) {
	//on sucess
	if (tr.WriteList[index].stage = Commit) {
		tr.WriteList[index].stage = commitDone
	}
}


// Reading a value -> ask from leaseholder ? 

//GenerateUUID ->relies on iota 

/*
	Verify if you can send the key with writeIntent if its alright current node since its replicated node
	if its sharded node, then we need to send the prepare message(with chances of abort to all the nodes)
*/

//SendPrepare


//SendCommit


//TimeOut for abort 
//Time
//abort the Tx with UUID 

// goRPC:HTTP:$PORT each leader 
//Leader / Service Discon
How to find TxManager Leader and KV-store Leader ?