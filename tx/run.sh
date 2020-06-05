#!/bin/sh
go build 
rm -rf raftexample*
 if [[ $?  -eq 0 ]] ; then
#modify procfile to change the way to run
goreman -b 5007 start
fi
