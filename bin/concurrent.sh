#!/bin/bash


BIN=`dirname $0`

LIB=$BIN/../lib

CLASSPATH=$BIN/../classes:$CLASSPATH

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

#java -classpath $CP -Dbind.address=192.168.2.5 org.jgroups.demos.Draw -props /home/bela/udp.xml &
#sleep 5

count=0
while [ $count -lt 20 ]
do
  echo "Starting Draw instance #$count"
  # change the IP address to your system
  java -classpath $CP -Dbind.address=192.168.1.5 org.jgroups.demos.Draw -props /home/bela/udp.xml &
  #  sleep 1
  count=$(($count+1))
done