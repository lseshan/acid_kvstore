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

	//"log"
	"net"
	"strings"
	"time"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"github.com/acid_kvstore/store/kvstore"
	log "github.com/pingcap-incubator/tinykv/log"
	"google.golang.org/grpc"
)

func checkLeader(ctx context.Context, kvs *kvstore.Kvstore) {
	log.Infof("check Leaderstarted")
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			s := kvs.Node.GetStatus()
			if kvs.Node.IsLeader(s) {
				log.Infof("Is leader")
			} else {
				log.Infof("Is not Leader")
			}
		case <-ctx.Done():
			log.Infof("Done with CheckLeader")
		}
	}
}

func setLogger(level string) {
	log.SetLevelByString(level)

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
}

func main() {
	//txcluster := flag.String("txcluster", "http://127.0.0.1:9021", "comma separated TxManager cluster peers")
	httport := flag.Int("httpport", 1024, "http server port")
	loglevel := flag.String("loglevel", "info", "info, warn, debug, error fatal")
	/* cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	*/
	grpcport := flag.String("grpcport", "127.0.0.1:9122", "grpc server port")
	flag.Parse()
	setLogger(*loglevel)
	//log.SetFlags(log.LstdFlags | log.Lshortfile)
	ctx := context.Background()

	ctx, cancel := context.WithCancel(ctx)

	var replica kvstore.Replica
	replica.Stores = make(map[uint64]*kvstore.Kvstore)
	replica.ReplicaName = *grpcport

	go replica.UpdateLeader(ctx)
	//	go checkLeader(ctx, kvs)

	/* RPC handling */
	go func() {
		log.Infof("grpx port %s", *grpcport)
		port := ":" + strings.Split(*grpcport, ":")[1]
		lis, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}
		log.Infof("Started listent")
		s := grpc.NewServer()
		pb.RegisterKvstoreServer(s, &replica)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	//kvs.ServeHttpKVApi(*kvport, errorC)
	replica.ServeHttpReplicaApi(*httport)
	cancel()

	// the key-value http handler will propose updates to raft
	//httpapi.ServeHttpKVAPI(*kvport, confChangeC, errorC)
	// start a rpc handler
	//rpc

}
