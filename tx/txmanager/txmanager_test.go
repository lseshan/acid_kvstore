package txmanager_test

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/tx/txmanager"
	"google.golang.org/grpc"
)

func TestTxAddCommand(t *testing.T) {

	rs := tr.TxAddCommand("Hey", "WhoisThis", "PUT")
	log.Printf("result of TxAdd:%t", rs)

	rs = tr.TxAddCommand("Hello", "This is Me", "PUT")
	log.Printf("result of TxAdd:%t", rs)
}

func TestTxSendBatchRequest(t *testing.T) {
	rs := tr.TxSendBatchRequest()
	log.Printf("result of TxAdd:%t", rs)

}

var tr *txmanager.TxRecord

const (
	address     = "localhost:50051"
	defaultName = "world"
)

func TestMain(m *testing.M) {
	// call flag parser if needed
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("grpc connection failed")
	}

	defer conn.Close()

	c := pb.NewKvstoreClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tr = txmanager.NewTxRecord(ctx, c)

	os.Exit(m.Run())

}
