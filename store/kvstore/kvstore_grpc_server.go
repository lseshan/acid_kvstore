package kvstore

import (
	"context"
	"log"
	"strconv"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Below implements gRPC for the kvstore
//func (kv *Kvstore) KvTxRead(_ context.Context, in *pb.KvStoreTxReadReq) (*pb.KvStoreTxReadReply, error) {
func (kv *Kvstore) KvTxRead(_ context.Context, in *pb.KvTxReadReq) (*pb.KvTxReadReply, error) {
	log.Printf("Reading the kvStoreTxRead: %d", in.Command.Key)
	//return nil, status.Errorf(codes.Unimplemented, "method KvStoreTxRead not implemented")
	cm := new(pb.Command)
	out := new(pb.KvTxReadReply)

	//out.txManagerId = 1
	//out.txRequestID = 2
	cm.Key = 1
	cm.Val = 2

	out.Command = cm
	/*	return &pb.KvStoreTxReadReply{ pb.TxContext{txManagerId: 1, txRequestId: 2},
		key:  "1" ,
		value: "1",
	}, nil */

	return out, nil

}

func (kv *Kvstore) KvTxPrepare(_ context.Context, in *pb.KvTxWriteReq) (*pb.KvTxWriteReply, error) {
	var txn Txn
	log.Printf("in kvtx prepare")
	txn.TxId = in.GetTxContext().GetTxId()
	txn.Cmd = "Prep"
	var oper operation
	oper.Optype = in.GetCommand().GetOp()
	oper.Key = strconv.FormatUint(in.GetCommand().GetKey(), 10)
	oper.Val = strconv.FormatUint(in.GetCommand().GetVal(), 10)
	txn.Oper = append(txn.Oper, oper)
	respCh := make(chan int)
	defer close(respCh)
	txn.RespCh = respCh
	kv.ProposeTxn(txn)
	log.Printf("Done propose txn")
	val := <-respCh
	log.Printf("Val %v", val)
	resp := pb.KvTxWriteReply{TxContext: in.GetTxContext(), Command: in.GetCommand()}
	if val == 1 {
		resp.Status = pb.Status_Success
	} else {
		resp.Status = pb.Status_Failure
	}
	log.Printf("Done success")

	return &resp, nil
}
func (kv *Kvstore) KvTxCommit(_ context.Context, in *pb.KvTxWriteReq) (*pb.KvTxWriteReply, error) {
	var txn Txn
	log.Printf("in kvtx prepare")
	txn.TxId = in.GetTxContext().GetTxId()
	txn.Cmd = "Commit"
	var oper operation
	oper.Optype = in.GetCommand().GetOp()
	oper.Key = strconv.FormatUint(in.GetCommand().GetKey(), 10)
	oper.Val = strconv.FormatUint(in.GetCommand().GetVal(), 10)
	txn.Oper = append(txn.Oper, oper)
	respCh := make(chan int)
	defer close(respCh)
	txn.RespCh = respCh
	kv.ProposeTxn(txn)
	log.Printf("Done Commit propose txn")
	val := <-respCh
	log.Printf("Val %v", val)
	resp := pb.KvTxWriteReply{TxContext: in.GetTxContext(), Command: in.GetCommand()}
	if val == 1 {
		resp.Status = pb.Status_Success
	} else {
		resp.Status = pb.Status_Failure
	}
	log.Printf("Done success")

	return &resp, nil
}
func (kv *Kvstore) KvRawRead(_ context.Context, in *pb.KvReadReq) (*pb.KvReadReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawRead not implemented")
}
func (kv *Kvstore) KvRawWrite(_ context.Context, in *pb.KvWriteReq) (*pb.KvWriteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawWrite not implemented")
}
func (kv *Kvstore) KvRawDelete(_ context.Context, in *pb.KvTxWriteReq) (*pb.KvTxWriteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawDelete not implemented")
}
