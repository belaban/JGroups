#!/bin/bash

## Measures context switches of the given program(s). Requires jps, top and tr

process=java

if [ -z "$1" ]
  then
    echo "No argument supplied, process=$process"
  else
     process=$1
fi

pids=`jps |grep ${process}|cut -d ' ' -f1|awk '{print "-pid " $1}' | tr '\n' ' '`

top -o CSW -c a -stats pid,cpu,command,threads,csw ${pids}