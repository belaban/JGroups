## Uses byteman to measure RTT times for UPerf

#!/bin/bash

PGM=org.jgroups.tests.perf.UPerf

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
LIB=`dirname $SCRIPT_DIR`/lib
SCRIPT=`dirname $SCRIPT_DIR`/conf/scripts/RttTest/rtt.btm
BM_OPTS="-Dorg.jboss.byteman.compile.to.bytecode=true"

jgroups.sh -javaagent:$LIB/byteman.jar=script:$SCRIPT $BM_OPTS $PGM $*
