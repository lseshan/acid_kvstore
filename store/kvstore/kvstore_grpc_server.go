package kvstore

import (
	"context"
	"log"

	pb "github.com/acid_kvstore/proto/package/kvstorepb"
)

type KvServer struct {
	//Kvstore *kvstore // Dont not need this if exported by package
	//	pb.kv *KvServer
	pb.UnimplementedKvstoreServer
}

//XXX:
func NewKvServer() *KvServer {
	log.Printf("New KvServer is Created")
	return &KvServer{}

}

// Below implements gRPC for the kvstore
//func (kv *KvServer) KvTxRead(_ context.Context, in *pb.KvStoreTxReadReq) (*pb.KvStoreTxReadReply, error) {
func (kv *KvServer) KvTxRead(_ context.Context, in *pb.KvTxReadReq) (*pb.KvTxReadReply, error) {
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

/*
XXX: Real implementations to be done here
func (kv *KvServer) KvTxPrepare(context.Context, in *pb.KvTxWriteReq) (*pb.KvTxWriteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvTxPrepare not implemented")
}
func (kv *KvServer) KvTxCommit(context.Context, in *pb.KvTxWriteReq) (*pb.KvTxWriteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvTxCommit not implemented")
}
func (kv *KvServer) KvTxRead(_ context.Context, in *pb.KvTxReadReq) (*pb.KvTxReadReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvTxRead not implemented")
}
func (kv *KvServer) KvRawRead(_ context.Context, in *pb.KvReadReq) (*pb.KvReadReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawRead not implemented")
}
func (kv *KvServer) KvRawWrite(_ context.Context, in *pb.KvWriteReq) (*pb.KvWriteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawWrite not implemented")
}
func (kv *KvServer) KvRawDelete(_ context.Context, in *pb.KvTxWriteReq) (*pb.KvTxWriteReply, error) {
	return nil, status.Errorf(codes.Unimplemented, "method KvRawDelete not implemented")
}

*/
