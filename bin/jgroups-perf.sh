## Uses byteman to measure times for individual protocols (up and down direction)

#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
LIB=`dirname $SCRIPT_DIR`/lib
SCRIPT=`dirname $SCRIPT_DIR`/conf/scripts/ProtPerf/prot-perf.btm

jgroups.sh -javaagent:$LIB/byteman.jar=script:$SCRIPT  $*
