#!/bin/sh

# Discovers all UDP-based members running on a certain mcast address (use -help for help)
# Probe [-help] [-addr <addr>] [-port <port>] [-ttl <ttl>] [-timeout <timeout>]


BIN=`dirname $0`

LIB=$BIN/../lib

LIBS=$LIB/log4j-1.2.6.jar:$LIB/commons-logging.jar:$LIB/concurrent.jar

#echo $CLASSPATH

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

java -classpath $CP org.jgroups.tests.Probe $*
