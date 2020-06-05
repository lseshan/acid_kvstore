package main

import (
	"context"
	"flag"
	"log"
	"net"
	"strings"

	pb "github.com/acid_kvstore/proto/package/replicamgrpb"
	replicamgr "github.com/acid_kvstore/replicaMgr/replica"
	"google.golang.org/grpc"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	shards := flag.Int("shards", 1, "number of shards")
	id := flag.Int("id", 1, "node ID")
	join := flag.Bool("join", false, "join an existing cluster")
	grpcport := flag.String("grpcport", ":9121", "grpc port")
	httport := flag.Int("httport", 9121, "http port")
	servers := flag.String("servers", "http://127.0.0.1:10221", "comma seperated replica servers")
	flag.Parse()
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	replicaMgr := replicamgr.NewReplicaMgr("127.0.0.1"+*grpcport, strings.Split(*cluster, ","), strings.Split(*servers, ","), *shards, *join, *id)

	ctx := context.Background()
	//start grpc server
	go func() {
		log.Printf("grpcport %v", *grpcport)
		lis, err := net.Listen("tcp", *grpcport)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterReplicamgrServer(s, &replicaMgr)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	//start http server
	go replicaMgr.ServeHttpReplicamgrApi(*httport)
	go replicaMgr.SendReplicaInfo(ctx)
	replicaMgr.Start()
	select {}

}
