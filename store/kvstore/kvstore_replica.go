package kvstore

import (
	"context"
	//"log"
	"time"

	replpb "github.com/acid_kvstore/proto/package/replicamgrpb"
	log "github.com/pingcap-incubator/tinykv/log"
)

func (repl *Replica) UpdateLeader(ctx context.Context) {
	log.Infof("Update leader")

	for {
		select {
		case <-time.After(5 * time.Second):
			if repl.Replclient == nil {
				continue
			}
			var out replpb.ReplicaUpdateReq
			var replica replpb.Replica
			replica.ShardMap = make(map[uint64]*replpb.Shard)
			replica.ReplicaName = repl.ReplicaName
			for i, stores := range repl.Stores {
				var shard replpb.Shard
				shard.LeaderKey = repl.ReplicaName
				s := stores.Node.GetStatus()
				shard.IsLeader = stores.Node.IsLeader(s)
				shard.ShardId = i
				replica.ShardMap[i] = &shard
			}
			out.ReplicaInfo = &replica
			_, err := repl.Replclient.ReplicaHeartbeat(context.Background(), &out)
			if err != nil {
				log.Infof("error in leader update: %v", err)
			} else {
				log.Infof("sent leader update")
			}

		case <-ctx.Done():
			log.Infof("Done with Update leader")
		}

	}

}
