#!/bin/bash

go build
if [[ $?  -eq 0 ]] ; then
    ./store --id 1 --cluster http://127.0.0.1:13379 --port 13380
fi
