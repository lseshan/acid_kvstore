#!/bin/sh
go build 
if [[ $?  -eq 0 ]] ; then ./Tx ; fi
