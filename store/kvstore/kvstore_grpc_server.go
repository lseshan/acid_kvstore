package kvstore

import (
	"context"
	//"log"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	log "github.com/pingcap-incubator/tinykv/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Below implements gRPC for the kvstore
//func (kv *Kvstore) KvTxRead(_ context.Context, in *pb.KvStoreTxReadReq) (*pb.KvStoreTxReadReply, error) {
/* func (kv *Kvstore) KvTxRead(_ context.Context, in *pb.KvTxReadReq) (*pb.KvTxReadReply, error) {
//	log.Infof("Reading the kvStoreTxRead: %d", in.Command.Key)
//return nil, status.Errorf(codes.Unimplemented, "method KvStoreTxRead not implemented")
cm := new(pb.Command)
out := new(pb.KvTxReadReply)

//out.txManagerId = 1
//out.txRequestID = 2
cm.Key = "1"
cm.Val = "2"

out.Command = cm
/*	return &pb.KvStoreTxReadReply{ pb.TxContext{txManagerId: 1, txRequestId: 2},
	key:  "1" ,
	value: "1",
}, nil */

/*	return out, nil

}
*/
func (kv *Kvstore) KvTxRead(_ context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {

	var readCommandResp []*pb.Command

	readcl := in.GetCommandList()
	for _, cm := range readcl {
		rkv, err := kv.KvHandleTxRead(cm.Key, in.TxContext.TxId)
		if err == nil {
			readCommandResp = append(readCommandResp, &pb.Command{Key: rkv.Key, Val: rkv.Val})
		} else {
			log.Fatalf("Error in return of value: %+v", rkv)
			resp := pb.KvTxReply{TxContext: in.GetTxContext(), CommandList: in.GetCommandList()}
			resp.Status = pb.Status_Failure
			return &resp, nil
		}

	}

	resp := pb.KvTxReply{TxContext: in.GetTxContext(), CommandList: readCommandResp}
	// XXX: val is 1 translated to sucess, might need to switch for uniformity
	resp.Status = pb.Status_Success
	log.Infof("Read success")

	return &resp, nil

}

func (kv *Kvstore) KvTxPrepare(_ context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	var txn Txn
	log.Infof("in kvtx prepare")
	txn.TxId = in.GetTxContext().GetTxId()
	txn.Cmd = "Prep"

	cl := in.GetCommandList()
	var oper operation
	for _, cm := range cl {
		oper.Optype = cm.Op
		oper.Key = cm.Key
		oper.Val = cm.Val
		log.Infof("Operation: %+v", oper)
		txn.Oper = append(txn.Oper, oper)
	}

	/* oper.Optype = in.GetCommand().GetOp()
	oper.Key = strconv.FormatUint(in.GetCommand().GetKey(), 10)
	oper.Val = strconv.FormatUint(in.GetCommand().GetVal(), 10)
	txn.Oper = append(txn.Oper, oper)
	*/
	respCh := make(chan int)
	defer close(respCh)
	txn.RespCh = respCh
	kv.ProposeTxn(txn)
	log.Infof("Done propose txn")
	val := <-respCh
	log.Infof("Val %v", val)
	resp := pb.KvTxReply{TxContext: in.GetTxContext(), CommandList: in.GetCommandList()}
	// XXX: val is 1 translated to sucess, might need to switch for uniformity
	if val == 1 {
		resp.Status = pb.Status_Success
	} else {
		resp.Status = pb.Status_Failure
	}
	log.Infof("Done success")

	return &resp, nil
}

func (kv *Kvstore) KvTxCommit(_ context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	var txn Txn
	log.Infof("in kvtx prepare")
	txn.TxId = in.GetTxContext().GetTxId()
	txn.Cmd = "Commit"
	cl := in.GetCommandList()
	var oper operation
	for _, cm := range cl {
		oper.Optype = cm.Op
		oper.Key = cm.Key
		oper.Val = cm.Val
		txn.Oper = append(txn.Oper, oper)
		log.Infof("Operation: %+v", oper)
	}

	/* var oper operation
	oper.Optype = in.GetCommand().GetOp()
	oper.Key = strconv.FormatUint(in.GetCommand().GetKey(), 10)
	oper.Val = strconv.FormatUint(in.GetCommand().GetVal(), 10)
	txn.Oper = append(txn.Oper, oper)
	*/
	respCh := make(chan int)
	defer close(respCh)
	txn.RespCh = respCh
	kv.ProposeTxn(txn)
	log.Infof("Done Commit propose txn")
	val := <-respCh
	log.Infof("Val %v", val)
	resp := pb.KvTxReply{TxContext: in.GetTxContext(), CommandList: in.GetCommandList()}
	if val == 1 {
		resp.Status = pb.Status_Success
	} else {
		resp.Status = pb.Status_Failure
	}
	log.Infof("Done success")

	return &resp, nil
}

func (kv *Kvstore) KvTxRollback(_ context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	var txn Txn
	log.Infof("KvTxRollback")
	txn.TxId = in.GetTxContext().GetTxId()
	txn.Cmd = "Abort"
	cl := in.GetCommandList()
	var oper operation
	for _, cm := range cl {
		oper.Optype = cm.Op
		oper.Key = cm.Key
		oper.Val = cm.Val
		txn.Oper = append(txn.Oper, oper)
		log.Infof("Operation: %+v", oper)
	}

	/* var oper operation
	oper.Optype = in.GetCommand().GetOp()
	oper.Key = strconv.FormatUint(in.GetCommand().GetKey(), 10)
	oper.Val = strconv.FormatUint(in.GetCommand().GetVal(), 10)
	txn.Oper = append(txn.Oper, oper)
	*/
	respCh := make(chan int)
	defer close(respCh)
	txn.RespCh = respCh
	kv.ProposeTxn(txn)
	log.Infof("Done Abort txn")
	val := <-respCh
	log.Infof("Val %v", val)
	resp := pb.KvTxReply{TxContext: in.GetTxContext(), CommandList: in.GetCommandList()}
	if val == 1 {
		resp.Status = pb.Status_Success
	} else {
		resp.Status = pb.Status_Failure
	}
	log.Infof("Done success")

	return &resp, nil
}

func (kv *Kvstore) KvRawRead(_ context.Context, in *pb.KvRawReq) (*pb.KvRawReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawRead not implemented")
}
func (kv *Kvstore) KvRawWrite(_ context.Context, in *pb.KvRawReq) (*pb.KvRawReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawWrite not implemented")
}
func (kv *Kvstore) KvRawDelete(_ context.Context, in *pb.KvRawReq) (*pb.KvRawReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawDelete not implemented")
}

//Replica

func (repl *Replica) KvTxRead(ctx context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	shardId := in.GetTxContext().GetShardId()
	if kv, ok := repl.Stores[shardId]; ok {
		return kv.KvTxRead(ctx, in)
	}
	return &pb.KvTxReply{Status: pb.Status_Failure}, nil

}

func (repl *Replica) KvTxPrepare(ctx context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	shardId := in.GetTxContext().GetShardId()
	if kv, ok := repl.Stores[shardId]; ok {
		return kv.KvTxPrepare(ctx, in)
	}
	return &pb.KvTxReply{Status: pb.Status_Failure}, nil
}
func (repl *Replica) KvReplicaJoin(_ context.Context, req *pb.ReplicaJoinReq) (*pb.ReplicaJoinReply, error) {
	log.Infof("Received join")

	repl.NewKVStoreWrapper(uint64(req.GetShardId()), int(req.GetShardId())*100+int(repl.Config.ReplicaId), req.GetConfig().GetPeers(), false)
	return &pb.ReplicaJoinReply{Status: pb.Status_Success}, nil
}
func (repl *Replica) KvReplicaLeave(_ context.Context, req *pb.ReplicaLeaveReq) (*pb.ReplicaLeaveReply, error) {
	return &pb.ReplicaLeaveReply{Status: pb.Status_Success}, nil
}
func (repl *Replica) KvTxCommit(ctx context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	shardId := in.GetTxContext().GetShardId()
	if kv, ok := repl.Stores[shardId]; ok {
		return kv.KvTxCommit(ctx, in)
	}
	return &pb.KvTxReply{Status: pb.Status_Failure}, nil
}

func (repl *Replica) KvTxRollback(ctx context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	shardId := in.GetTxContext().GetShardId()
	if kv, ok := repl.Stores[shardId]; ok {
		return kv.KvTxRollback(ctx, in)
	}
	return &pb.KvTxReply{Status: pb.Status_Failure}, nil
}
func (repl *Replica) KvRawRead(_ context.Context, in *pb.KvRawReq) (*pb.KvRawReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawRead not implemented")
}
func (repl *Replica) KvRawWrite(_ context.Context, in *pb.KvRawReq) (*pb.KvRawReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawWrite not implemented")
}
func (repl *Replica) KvRawDelete(_ context.Context, in *pb.KvRawReq) (*pb.KvRawReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawDelete not implemented")
}

func (repl *Replica) KvReplicaUpdateConfig(_ context.Context, in *pb.ReplicaConfigReq) (*pb.ReplicaConfigResp, error) {
	localConfig := in.GetConfig()

	if repl.Config == nil {
		repl.Config = localConfig
		go repl.StartReplMgrGrpcClient()
	} else if repl.Config.ReplLeader != localConfig.ReplLeader {
		repl.Conn.Close()
		repl.Replclient = nil
		repl.Config.ReplLeader = localConfig.ReplLeader
		go repl.StartReplMgrGrpcClient()
	}

	if TxManager == nil {
		if localConfig.TxLeader != "" {
			NewKvTxManager([]string{localConfig.TxLeader})
			//To debug this info is stored here
			repl.Config.TxLeader = localConfig.TxLeader
		}
	} else if TxManager.TxLeader != localConfig.TxLeader {
		TxManager.KvCloseTxClient()
		NewKvTxManager([]string{localConfig.TxLeader})
		repl.Config.TxLeader = localConfig.TxLeader
	} else {
		log.Infof("No change in TxConfig")
	}

	//TODO:Handle TxLeader
	return &pb.ReplicaConfigResp{Status: pb.Status_Success}, nil
}

func (repl *Replica) KvShardUpdateConfig(_ context.Context, in *pb.ShardConfigReq) (*pb.ShardConfigResp, error) {
	for _, shardConfig := range in.GetConfig() {
		if _, ok := repl.Stores[uint64(shardConfig.GetShardId())]; !ok {
			log.Infof("Add new shard")
			if repl != nil {
				if repl.Config != nil {
					repl.NewKVStoreWrapper(uint64(shardConfig.GetShardId()), int(shardConfig.GetShardId())*100+int(repl.Config.ReplicaId), shardConfig.GetPeers(), false)
				}
			}
		}

	}
	return nil, status.Errorf(codes.Unimplemented, "method KvShardUpdateConfig not implemented")
}
