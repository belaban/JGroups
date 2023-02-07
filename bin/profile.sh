#!/bin/bash


SCRIPT=conf/scripts/profile.btm

if [ $# -lt 1 ];
  then
     echo "profile.sh classname [args]"
fi

PGM=$1
shift

bm.sh $PGM $SCRIPT $*


