#!/bin/sh

# Determines the fragmentation size of your system
# Set FRAG.frag_size and pbcast.NAKACK.max_xmit_size to the value

BIN=`dirname $0`

CLASSPATH=$BIN/../classes

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


java -cp $CP org.jgroups.tests.DetermineFragSize
