// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"log"
	"net"
	"time"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/store/kvstore"
	"google.golang.org/grpc"
)

const (
	gRPCport = ":50051"
)

func checkLeader(ctx context.Context, kvs *kvstore.Kvstore) {
	log.Printf("check Leaderstarted")
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			if kvs.Node.IsLeader() {
				log.Printf("Is leader")
			} else {
				log.Printf("Is not Leader")
			}
		case <-ctx.Done():
			log.Printf("Done with CheckLeader")
		}
	}
}

func main() {
	httport := flag.Int("httpport", 1024, "http server port")
	grpcport := flag.String("grpcport", ":9122", "grpc server port")
	flag.Parse()
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)

	var replica kvstore.Replica
	replica.Stores = make(map[uint64]*kvstore.Kvstore)

	go replica.UpdateLeader(ctx)

	/* RPC handling */
	go func() {
		lis, err := net.Listen("tcp", *grpcport)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		s := grpc.NewServer()
		pb.RegisterKvstoreServer(s, &replica)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	replica.ServeHttpReplicaApi(*httport)
	cancel()

	// the key-value http handler will propose updates to raft
	//httpapi.ServeHttpKVAPI(*kvport, confChangeC, errorC)
	// start a rpc handler
	//rpc

}
