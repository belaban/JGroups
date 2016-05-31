# Author: Bela Ban

#!/bin/bash

JG=${JG-$HOME/JGroups}

LIB=$JG/lib

CP=$JG/classes:$JG/conf

# If this is a bin dist, JARs are in the $JG directory.
if [ ! -d $LIB ]; then
    LIB=$JG
fi;

for i in $LIB/*.jar
do
    CP=$CP:$i
done

if [ -f $HOME/log4j.properties ]; then
    LOG="-Dlog4j.configuration=file:$HOME/log4j.properties"
fi;

JG_FLAGS="-Dresolve.dns=false -Djgroups.bind_addr=$IP_ADDR -Djboss.tcpping.initial_hosts=$IP_ADDR[7800]"
JG_FLAGS="$JG_FLAGS -Djava.net.preferIPv4Stack=true -Djgroups.timer.num_threads=4"
FLAGS="-server -Xmx600M -Xms600M"
FLAGS="$FLAGS -XX:CompileThreshold=10000 -XX:+AggressiveHeap -XX:ThreadStackSize=64K -XX:SurvivorRatio=8"
FLAGS="$FLAGS -XX:TargetSurvivorRatio=90"
FLAGS="$FLAGS -Xshare:off"
# JMX="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=7777 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
JMX="-Dcom.sun.management.jmxremote"
#EXPERIMENTAL="-XX:+UseFastAccessorMethods -XX:+UseTLAB"

#EXPERIMENTAL="$EXPERIMENTAL -XX:+DoEscapeAnalysis -XX:+EliminateLocks -XX:+UseBiasedLocking"
EXPERIMENTAL="$EXPERIMENTAL -XX:+EliminateLocks -XX:+UseBiasedLocking"

#EXPERIMENTAL="$EXPERIMENTAL -XX:+AggressiveOpts -XX:+DoEscapeAnalysis -XX:+EliminateLocks -XX:+UseBiasedLocking -XX:+UseCompressedOops"
#EXPERIMENTAL="$EXPERIMENTAL -XX:+UnlockExperimentalVMOptions -XX:+UseG1GC"

#java -Xrunhprof:cpu=samples,monitor=y,interval=5,lineno=y,thread=y -classpath $CP $LOG $JG_FLAGS $FLAGS $EXPERIMENTAL $JMX  $*

#DEBUG="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5000"

java -classpath $CP $DEBUG $LOG $JG_FLAGS $FLAGS $EXPERIMENTAL $JMX  $*

