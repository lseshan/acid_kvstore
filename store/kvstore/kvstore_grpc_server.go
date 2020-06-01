package kvstore

import (
	"context"
	"log"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Below implements gRPC for the kvstore
//func (kv *Kvstore) KvTxRead(_ context.Context, in *pb.KvStoreTxReadReq) (*pb.KvStoreTxReadReply, error) {
/* func (kv *Kvstore) KvTxRead(_ context.Context, in *pb.KvTxReadReq) (*pb.KvTxReadReply, error) {
//	log.Printf("Reading the kvStoreTxRead: %d", in.Command.Key)
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

func (kv *Kvstore) KvTxPrepare(_ context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	var txn Txn
	log.Printf("in kvtx prepare")
	txn.TxId = in.GetTxContext().GetTxId()
	txn.Cmd = "Prep"

	cl := in.GetCommandList()
	var oper operation
	for _, cm := range cl {
		oper.Optype = cm.Op
		oper.Key = cm.Key
		oper.Val = cm.Val
		log.Printf("Operation: %+v", oper)
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
	log.Printf("Done propose txn")
	val := <-respCh
	log.Printf("Val %v", val)
	resp := pb.KvTxReply{TxContext: in.GetTxContext(), CommandList: in.GetCommandList()}
	if val == 1 {
		resp.Status = pb.Status_Success
	} else {
		resp.Status = pb.Status_Failure
	}
	log.Printf("Done success")

	return &resp, nil
}

func (kv *Kvstore) KvTxCommit(_ context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	var txn Txn
	log.Printf("in kvtx prepare")
	txn.TxId = in.GetTxContext().GetTxId()
	txn.Cmd = "Commit"
	cl := in.GetCommandList()
	var oper operation
	for _, cm := range cl {
		oper.Optype = cm.Op
		oper.Key = cm.Key
		oper.Val = cm.Val
		txn.Oper = append(txn.Oper, oper)
		log.Printf("Operation: %+v", oper)
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
	log.Printf("Done Commit propose txn")
	val := <-respCh
	log.Printf("Val %v", val)
	resp := pb.KvTxReply{TxContext: in.GetTxContext(), CommandList: in.GetCommandList()}
	if val == 1 {
		resp.Status = pb.Status_Success
	} else {
		resp.Status = pb.Status_Failure
	}
	log.Printf("Done success")

	return &resp, nil
}

func (kv *Kvstore) KvTxRead(_ context.Context, in *pb.KvTxReadReq) (*pb.KvTxReadReply, error) {

	return nil, status.Errorf(codes.Unimplemented, "method KvTxRead not implemented")
}

func (kv *Kvstore) KvTxRollback(_ context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvTxRollback not implemented")
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

func (repl *Replica) KvTxPrepare(ctx context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	shardId := in.GetTxContext().GetShardId()
	if kv, ok := repl.Stores[shardId]; ok {
		return kv.KvTxPrepare(ctx, in)
	}
	return &pb.KvTxReply{Status: pb.Status_Failure}, nil
}
func (repl *Replica) KvReplicaJoin(_ context.Context, req *pb.ReplicaJoinReq) (*pb.ReplicaJoinReply, error) {
	repl.NewKVStoreWrapper(req.GetShardId(), 1, req.GetConfig().GetPeers(), true)
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

func (repl *Replica) KvTxRead(_ context.Context, in *pb.KvTxReadReq) (*pb.KvTxReadReply, error) {

	return nil, status.Errorf(codes.Unimplemented, "method KvTxRead not implemented")
}
func (repl *Replica) KvTxRollback(_ context.Context, in *pb.KvTxReq) (*pb.KvTxReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvTxRollback not implemented")
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
	repl.Config = in.GetConfig()
	repl.StartReplMgrGrpcClient()
	//ToDo:Start Grpc client to ReplicManager
	return &pb.ReplicaConfigResp{Status: pb.Status_Success}, nil
}

func (repl *Replica) KvShardUpdateConfig(_ context.Context, in *pb.ShardConfigReq) (*pb.ShardConfigResp, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvShardUpdateConfig not implemented")
}
