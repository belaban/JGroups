#!/bin/bash

#Author: Pedro Ruivo
#Since: 3.1
#Note: I'm assuming that jgroups-<version>.jar is in the same directory as this script. The same applies for
#      for the log4j.properties

WORKING_DIR=`cd $(dirname $0); pwd`
HOSTNAME=`hostname`
CP="${WORKING_DIR}/jgroups*.jar"

#enable remote JMX
JMX="-Dcom.sun.management.jmxremote.port=8081 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

#jgroups flags
JG_FLAGS="-Dresolve.dns=false -Djgroups.bind_addr=${HOSTNAME} -Djgroups.timer.num_threads=4"

#log4j
LOG4J="-Dlog4j.configuration=file:${WORKING_DIR}/log4j.properties"

#java flags
JAVA_FLAGS="-server -Xmx7G -Xms7G"

while [ -n "$1" ]; do
case $1 in
  -h) HELP="1"; break;;
  -test-order) ORDER="1"; shift 1; break;;
  -nr-nodes) NR_NODES=$2; shift 2;;
  -nr-messages) NR_MESSAGES=$2; shift 2;;
  -config) CONFIG=$2; shift 2;;
  -*) echo "unknown option $1"; shift 1;;
  *) echo "unknown argument $1"; shift 1;;
esac;
done

if [ -n "${HELP}" ]; then
ARGS="-h"
else if [ -n "${ORDER}" ]; then
CMD="java ${JAVA_FLAGS} -cp ${CP} ${JMX} ${JG_FLAGS} ${LOG4J} org.jgroups.tests.CheckToaOrder $*"
echo ${CMD}
${CMD} > ${WORKING_DIR}/check_std_out_${HOSTNAME}.out 2>&1 &
exit 0
ARGS="-test-order $*"
else
if [ -n "${NR_NODES}" ]; then
ARGS="-nr-nodes ${NR_NODES}"
fi
if [ -n "${NR_MESSAGES}" ]; then
ARGS="${ARGS} -nr-messages ${NR_MESSAGES}"
fi
if [ -n "${CONFIG}" ]; then
ARGS="${ARGS} -config ${CONFIG}"
fi
fi
fi

CMD="java ${JAVA_FLAGS} -cp ${CP} ${JMX} ${JG_FLAGS} ${LOG4J} org.jgroups.tests.TestToaOrder ${ARGS}"

echo ${CMD}
${CMD} > ${WORKING_DIR}/std_out_${HOSTNAME}.out 2>&1 &
