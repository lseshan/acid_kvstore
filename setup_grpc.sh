
#intel
#https://grpc.io/docs/quickstart/go/
#set GOPATH=$HOME/go/
#set PATH=$PATH:$GOPATH/bin
#from root of the project (acid_kvstore) file
protoc -I proto/ proto/proto/kv-storepb.proto --go_out=plugins=grpc:proto
#protoc -I proto/ proto/proto/txmanagerpb.proto --go_out=plugins=grpc:proto
# -I <root of working directory 
# 

