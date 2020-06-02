package txmanager

import (
	"log"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"google.golang.org/grpc"
)

/*
XXX:Finds the alive and closer Client to talk to

Currently it keeps track of leader and get information from leader


*/
type TxKvManager struct {
	ClientAddress []string
	KvLeader      string
	Cli           pb.KvstoreClient
	conn          *grpc.ClientConn
}

var KvClient *TxKvManager

// XXX: Check if race can happen
func NewTxKvManager(s []string, compl chan int) {

	t := new(TxKvManager)
	t.ClientAddress = s
	t.KvLeader = s[0]
	t.TxKvCreateClientCtx()

	KvClient = t
	compl <- 1
	log.Printf("KVCli context setup")
}

func (t *TxKvManager) TxKvGetClient() pb.KvstoreClient {
	return t.Cli
}

func (t *TxKvManager) KvCloseTxClient() {

	t.conn.Close()
}

func (t *TxKvManager) TxKvAddNode() {

}
func (t *TxKvManager) TxKvDelNode() {

}

func (t *TxKvManager) TxKvUpdateLeader() {

}

func (t *TxKvManager) TxKvCreateClientCtx() {
	// XXX: Got to find better way
	if len(t.KvLeader) > 0 {
		log.Printf("Leader:%s", t.KvLeader)
		conn, err := grpc.Dial(t.KvLeader, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("grpc connection failed")
		}

		t.Cli = pb.NewKvstoreClient(conn)
		t.conn = conn

	}

}

func (t *TxKvManager) TxFindNearClient() {}
