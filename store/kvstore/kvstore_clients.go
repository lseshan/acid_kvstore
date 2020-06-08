package kvstore

import (
	//"log"

	pbt "github.com/acid_kvstore/proto/package/txmanagerpb"
	log "github.com/pingcap-incubator/tinykv/log"
	"google.golang.org/grpc"
)

/*
XXX:Finds the alive and closer Client to talk to

Currently it keeps track of leader and get information from leader


*/
type KvTxManager struct {
	ClientAddress []string
	TxLeader      string
	cli           pbt.TxmanagerClient
	conn          *grpc.ClientConn
}

var TxManager *KvTxManager

// XXX: Check if race can happen
func NewKvTxManager(s []string) {
	t := new(KvTxManager)
	t.ClientAddress = s
	t.TxLeader = s[0]
	t.KvTxCreateClientCtx()

	TxManager = t
}

func (t *KvTxManager) KvTxGetClient() pbt.TxmanagerClient {
	return t.cli
}

func (t *KvTxManager) KvCloseTxClient() {

	t.conn.Close()
}

func (t *KvTxManager) KvTxAddNode() {

}
func (t *KvTxManager) KvTxDelNode() {

}

func (t *KvTxManager) KvTxUpdateLeader() {

}

func (t *KvTxManager) KvTxCreateClientCtx() {
	// XXX: Got to find better way
	if len(t.TxLeader) > 0 {
		conn, err := grpc.Dial(t.TxLeader, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("grpc connection failed")
		}

		t.cli = pbt.NewTxmanagerClient(conn)
		t.conn = conn

	}

}

func (t *KvTxManager) TxFindNearClient() {}
