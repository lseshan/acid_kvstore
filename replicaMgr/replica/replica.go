package replicamgr

import (
	"context"
	"encoding/json"
	"log"
	"strconv"
	"strings"
	"time"

	kvpb "github.com/acid_kvstore/proto/package/kvstorepb"
	pb "github.com/acid_kvstore/proto/package/replicamgrpb"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	"github.com/acid_kvstore/raft"
	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/raft/raftpb"
)

type Server struct {
	ServerKey   string //combination of server ip:port
	ShardInfo   []*pb.Shard
	ShardConfig map[int32]*kvpb.ShardConfig
	Client      kvpb.KvstoreClient
	Peers       []string
	ReplicaId   uint32
}
type ReplicaMgr struct {
	ProposeC    chan<- string
	Servers     map[string]Server
	serverlist  []string
	Shards      int // number of shards
	Node        *raft.RaftNode
	Shard       map[uint64]*pb.Shard //
	CommitC     <-chan string
	Snapshotter *snap.Snapshotter
	MyName      string
	TxInfo      pb.TxInfo
}

func (repl *ReplicaMgr) GetSnapshot() ([]byte, error) {
	return json.Marshal(repl.Shard)
}

func NewReplicaMgr(name string, cluster []string, servers []string, shards int, join bool, id int) ReplicaMgr {
	proposeC := make(chan string)
	confChangeC := make(chan raftpb.ConfChange)

	var replicamgr ReplicaMgr

	getSnapshot := func() ([]byte, error) { return replicamgr.GetSnapshot() }

	_, _, snapshotterReady, rc := raft.NewRaftNode(1, []raft.PeerInfo{raft.PeerInfo{Id: 1, Peer: cluster[0]}}, join, getSnapshot, proposeC, confChangeC)
	replicamgr.Node = rc
	replicamgr.Servers = make(map[string]Server)
	replicamgr.Shard = make(map[uint64]*pb.Shard)
	replicamgr.ProposeC = proposeC
	replicamgr.Snapshotter = <-snapshotterReady
	replicamgr.MyName = name
	replicamgr.serverlist = servers
	replicamgr.Shards = shards

	return replicamgr
}

func (repl *ReplicaMgr) Start() {

	//For each server send ReplicaInformation
	log.Printf("serverlist:%v", repl.serverlist)
	for i, servername := range repl.serverlist {
		localServer := Server{}
		localServer.ShardConfig = make(map[int32]*kvpb.ShardConfig)
		var shardportstart int

		shardportstart = 22345

		//Shard start = 1 22345  22346  22347
		//                22348
		//				3 106
		for j := 0; j < repl.Shards; j++ {

			var peerServers []string
			for i, servers := range repl.serverlist {
				serverIp := strings.Split(servers, ":")[0]
				peerServers = append(peerServers, "http://"+serverIp+":"+strconv.Itoa(shardportstart+j*len(repl.serverlist)+i))
			}
			log.Printf("%v", peerServers)
			localServer.ShardConfig[int32(j+1)] = &kvpb.ShardConfig{Peers: peerServers, ShardId: int32(j + 1)}
		}

		localServer.ServerKey = servername
		localServer.ReplicaId = uint32(i + 1)
		localServer.Peers = repl.serverlist
		repl.Servers[servername] = localServer
		repl.StartServerConnection(context.Background(), repl.Servers[servername])
	}
	//time.Sleep(10 * time.Second)
	for _, servername := range repl.serverlist {
		repl.SendReplicaInformation(repl.Servers[servername])
		for j := 0; j < repl.Shards; j++ {
			repl.SendShardJoinInformation(repl.Servers[servername], int32(j+1))
		}
		//replicamgr.SendShardJoinInformation(replicamgr.Servers[servername], 2)
		//replicamgr.SendShardJoinInformation(replicamgr.Servers[servername], 3)

	}
	//For each server send Shard Join
}

func (repl *ReplicaMgr) SendReplicaInformation(Server Server) {
	var out kvpb.ReplicaConfigReq
	out.Config = &kvpb.ReplicaConfig{TxLeader: repl.TxInfo.RpcEndpoint, ReplLeader: repl.MyName, ReplicaId: Server.ReplicaId, Nshards: uint32(repl.Shards)}
	if Server.Client != nil {
		_, _ = Server.Client.KvReplicaUpdateConfig(context.Background(), &out)
		log.Printf("done sending replica information")
	}
}

func (repl *ReplicaMgr) SendShardJoinInformation(Server Server, id int32) {
	var out kvpb.ReplicaJoinReq

	out.ShardId = id
	out.Config = Server.ShardConfig[id]
	_, _ = Server.Client.KvReplicaJoin(context.Background(), &out)

}

func (repl *ReplicaMgr) StartServerConnection(ctx context.Context, Server Server) {
	log.Printf("Trying to connect")
	conn, err := grpc.Dial(Server.ServerKey, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	//defer conn.Close()
	cli := kvpb.NewKvstoreClient(conn)
	localServer := Server
	localServer.Client = cli
	repl.Servers[Server.ServerKey] = localServer
	log.Printf("server connected")
}

// Routine to Send information to Replica
//   --> Send Join, SendLeave
//   --> On leader Change Send ReplicaInfomation

// Routine to receive information from Replica
func (repl *ReplicaMgr) ReplicaHeartbeat(ctx context.Context, in *pb.ReplicaUpdateReq) (*empty.Empty, error) {
	log.Printf("received Kv heartbeat")
	//ReplicaKey := in.GetReplicaInfo().GetReplicaName()
	shardMap := in.GetReplicaInfo().GetShardMap()
	for id, shard := range shardMap {
		if shard.GetIsLeader() {
			repl.Shard[id] = shard
		}
	}
	return new(empty.Empty), nil
	//Store the shard details in repl.Server. Not sure if we need it now
}

func (repl *ReplicaMgr) ReplicaTxLeaderHeartBeat(ctx context.Context, in *pb.ReplicaTxReq) (*empty.Empty, error) {
	log.Printf("received TxLeader heartbeat")
	repl.TxInfo = *in.GetTxInfo()
	return new(empty.Empty), nil
}

func (repl *ReplicaMgr) ReplicaQuery(ctx context.Context, in *pb.ReplicaQueryReq) (*pb.ReplicaQueryResp, error) {
	log.Printf("received ReplicaQuery")
	var out pb.ReplicaQueryResp
	out.TxInfo = &repl.TxInfo
	out.ShardInfo = &pb.ShardInfo{ShardMap: repl.Shard}
	out.ReplicaInfo = &pb.ReplicaInfo{Nshards: uint32(repl.Shards)}
	return &out, nil
	//out.ShardInfo = repl.ShardInfo
	//out.ReplicaInfo = repl.ReplicaInfo
}

func (repl *ReplicaMgr) SendReplicaInfo(ctx context.Context) {
	shardtimeout := time.After(2 * time.Second)
	for {
		select {
		case <-shardtimeout:
			//Send shard info to servers : needed in case of server crash and joins.
			//ToDo:We need a health check service which checks for the state and then pushes the config
			log.Printf("Sending periodic shard info")
			for _, servername := range repl.serverlist {
				if server, ok := repl.Servers[servername]; ok {
					var out kvpb.ShardConfigReq
					for id, _ := range server.ShardConfig {
						out.Config = append(out.Config, server.ShardConfig[id])
					}
					if server.Client != nil {
						server.Client.KvShardUpdateConfig(context.Background(), &out)
						log.Printf("done sending shard information to server %s", server.ServerKey)

					}
				}
			}
			shardtimeout = time.After(2 * time.Second)
		case <-time.After(1 * time.Second):
			//Send to replica Server
			for _, servername := range repl.serverlist {
				if server, ok := repl.Servers[servername]; ok {
					repl.SendReplicaInformation(server)
				}
			}

		case <-ctx.Done():
			log.Printf("close SendReplicaInfo")
			return

		}

	}

}
