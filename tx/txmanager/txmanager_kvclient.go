package txmanager

import (
	"log"
	"strings"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"google.golang.org/grpc"
)

/*
XXX:Finds the alive and closer Client to talk to

Currently it keeps track of leader and get information from leader


*/
type TxKvManager struct {
	shardServer string
	Cli         pb.KvstoreClient
	Conn        *grpc.ClientConn
}

//KvClient[server] = TxKvManager
var KvClient map[string]*TxKvManager

// XXX: Check if race can happen
func InitTxKvMapper() {
	KvClient = make(map[string]*TxKvManager)
}

/*
func NewTxKvManager(s []string, compl chan int) {

	KvClient = make(map[string]*TxKvManager)

	for _, server := range s {
		t := new(TxKvManager)
		t.shardServer = server
		t.TxKvCreateClientCtx(server)
		KvClient[server] = t
	}
	compl <- 1
	log.Printf("grpc client for shardservers are setup")
}
*/

/*
func (t *TxKvManager) KvCloseTxClient(s string) {

	t.Conn.Close()
}

func (t *TxKvManager) TxKvAddNode() {

}
func (t *TxKvManager) TxKvDelNode() {

}

func (t *TxKvManager) TxKvUpdateLeader() {

}

func (t *TxKvManager) TxKvCloseConn(s string) {

}
*/
/*
func (t *TxKvManager) TxKvUpdateCtxForServer(s string) {
	// XXX: Got to find better way
	if len(s) > 0 {
		log.Printf("Creating grpc conn for Server:%s", s)
		conn, err := grpc.Dial(s, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Fatalf("ERROR: grpc connection failed")
			return
		}
		t.Cli = pb.NewKvstoreClient(conn)
		t.Conn = conn
	}
}
*/
func TxKvCreateClientCtx(s string) {
	// XXX: Got to find better way
	port := strings.Split(s, ":")
	grpcServer := "localhost:"
	ip := grpcServer + port[1]
	log.Printf("GRPC connection server:%v", ip)
	if _, ok := KvClient[s]; ok == true {
		log.Printf("Ctx is alreay present: Resulting in noperation")
	}
	t := new(TxKvManager)
	t.shardServer = s

	conn, err := grpc.Dial(ip, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("ERROR: grpc connection failed")
		return
	}
	t.Cli = pb.NewKvstoreClient(conn)
	t.Conn = conn

}

///func (t *TxKvManager) TxFindNearClient() {}
