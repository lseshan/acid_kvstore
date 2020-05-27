#!/bin/bash

go build
if [[ $?  -eq 0 ]] ; then
    ./store --id 1 --cluster http://127.0.0.1:13379,http://127.0.0.1:14479,http://127.0.0.1:15479  --port 13380 --grpcport :50051 &
    ./store --id 2 --cluster http://127.0.0.1:13379,http://127.0.0.1:14479,http://127.0.0.1:15479  --port 14480 --grpcport :50052 &
    ./store --id 3 --cluster http://127.0.0.1:13379,http://127.0.0.1:14479,http://127.0.0.1:15479  --port 15480 --grpcport :50053 &
fi
