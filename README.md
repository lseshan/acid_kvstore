ACID KV Store
============
ReplicMgr
./replicamgr --id 1 --cluster http://127.0.0.1:12379 --servers 127.0.0.1:22379,127.0.0.1:22380,127.0.0.1:22381 --grpcport 127.0.0.1:21224 --httport 1026 --shards 3

KVstore:
./store --grpcport  127.0.0.1:22379 --httpport 1024
./store  --grpcport 127.0.0.1:22381 --httpport 1027
./store  --grpcport 127.0.0.1:22381 --httpport 1025

TxStore:
./tx --id 2   --cluster http://127.0.0.1:23479 --cliport 23480 --grpcport 127.0.0.1:20051 --replicamgrs 127.0.0.1:21224


How to run GoTest Cases:
======================
go test -run  TestMultipleConcurrentWriteTxnDifferentKeyScale -v
go test -list . 

Query Commands use curl or via browser:
======================================
Replmgr:   http://<replmgrip:replport>/api/replica
Txmgr:     http://<txmgrip:txmgrhttp>/api/txmgrquery
Kvstore:   http://<kvstoreip:kvhttpport>/api/replicaconfig/

