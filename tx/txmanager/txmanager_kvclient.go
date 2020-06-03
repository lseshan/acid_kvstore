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
	Conn          *grpc.ClientConn
	AllClient     map[string]pb.KvstoreClient
	AllConn       map[string]*grpc.ClientConn
}

var KvClient *TxKvManager

// XXX: Check if race can happen
func NewTxKvManager(s []string, compl chan int) {

	t := new(TxKvManager)
	t.AllClient = make(map[string]pb.KvstoreClient)
	t.AllConn = make(map[string]*grpc.ClientConn)
	t.ClientAddress = s
	t.KvLeader = s[0]
	for _, servers := range t.ClientAddress {
		t.TxKvCreateClientCtx(servers)
	}
	t.Cli = t.AllClient[s[0]]
	t.Conn = t.AllConn[s[0]]

	KvClient = t
	compl <- 1
	log.Printf("KVCli context setup")
}

func (t *TxKvManager) TxKvGetClient() pb.KvstoreClient {
	return t.Cli
}

func (t *TxKvManager) KvCloseTxClient() {

	t.Conn.Close()
}

func (t *TxKvManager) TxKvAddNode() {

}
func (t *TxKvManager) TxKvDelNode() {

}

func (t *TxKvManager) TxKvUpdateLeader() {

}

func (t *TxKvManager) TxKvCreateClientCtx(s string) {
	// XXX: Got to find better way
	if len(s) > 0 {
		log.Printf("Server:%s", s)
		conn, err := grpc.Dial(s, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("grpc connection failed")
		}

		t.AllClient[s] = pb.NewKvstoreClient(conn)
		t.AllConn[s] = conn

	}

}

func (t *TxKvManager) TxFindNearClient() {}
