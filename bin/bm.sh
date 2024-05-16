## Uses byteman plus script to run a program

#!/bin/bash

if [ $# -lt 2 ];
    then echo "bm.sh classname byteman-script";
         exit 1
fi

PGM=$1
SCRIPT=$2

if [ ! -f $SCRIPT ]; then
   echo "** Script $SCRIPT not found **"
   exit 1
fi

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
LIB=`dirname $SCRIPT_DIR`/lib
BM_OPTS="-Dorg.jboss.byteman.compile.to.bytecode=true"

shift
shift


jgroups.sh -javaagent:$LIB/byteman.jar=script:$SCRIPT $BM_OPTS $PGM $*
