#!/bin/bash


LIB=../lib

LIBS=$LIB/log4j.jar:$LIB/commons-logging.jar

FLAGS="$FLAGS -Djava.net.preferIPv4Stack=true -Djgroups.bind_addr=192.168.1.5 -Djgroups.tcpping.initial_hosts=192.168.1.5[7800]"

CLASSPATH=../classes:../conf:$CLASSPATH:$LIBS

# OS specific support (must be 'true' or 'false').
cygwin=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;
esac

if [ $cygwin = "true" ]; then
   CP=`cygpath -wp $CLASSPATH`
else
   CP=$CLASSPATH
fi


count=0
while [ $count -lt 20 ]
do
  echo "Starting Draw instance #$count"
  java -Ddisable_canonicalization=false -classpath $CP $LOG $FLAGS -Dcom.sun.management.jmxremote -Dresolve.dns=false org.jgroups.demos.Draw -props $HOME/udp-2.6.xml &
  # sleep 1
  count=$(($count+1))
done