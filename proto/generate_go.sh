protoc -I proto/ proto/kv-storepb.proto --go_out=plugins=grpc:proto
protoc -I proto/ proto/txmanagerpb.proto --go_out=plugins=grpc:proto
protoc -I proto/ proto/replicamgr.proto --go_out=plugins=grpc:proto
