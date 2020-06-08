#!/bin/sh
#./run.sh <port>
if [ -z "$1" ]
then
       go test -run TestSimpleReadTxn   
       go test -run TestSimpleReadWriteTxn  
       go test -run TestSimpleReadTxn  
     go test -run TestSimpleWriteTxn
 else 
    if [ $1 = "1" ] 
    then
        port="23480"
    fi
    if [ $1 = "2" ] 
    then
        port="24480"
    fi
    if [ $1 = "3" ] 
    then
        port="25480"
    fi
    echo $port
   
   go test -run TestSimpleWriteTxn -args -port=$port 
   go test -run TestSimpleReadTxn -args -port=$port  
   go test -run TestSimpleReadWriteTxn -args -port=$port  
   go test -run TestSimpleReadTxn -args -port=$port  
   #go test -args -port=$port  
fi

