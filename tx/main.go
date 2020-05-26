package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"google.golang.org/grpc"
)

const (
	address     = "localhost:50051"
	defaultName = "world"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	// Set up a connection to the server.
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewKvstoreClient(conn)

	// Contact the server and print out its response.
	/* name := defaultName
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	*/
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ts := NewTxStore(ctx, c, snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error)

	ts.ServeHttpTxApi(*kvport, errorC)
}
