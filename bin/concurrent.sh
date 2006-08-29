#!/bin/bash


BIN=`dirname $0`

LIB=$BIN/../lib

LIBS=$LIB/log4j-1.2.6.jar:$LIB/commons-logging.jar:$LIB/concurrent.jar

echo $CLASSPATH

CLASSPATH=$BIN/../classes:$CLASSPATH:$LIBS

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
  java -classpath $CP -Dbind.address=192.168.5.2 org.jgroups.demos.Draw -props c:\\fc-fast-minimalthreads.xml &
  sleep 1
  count=$(($count+1))
done