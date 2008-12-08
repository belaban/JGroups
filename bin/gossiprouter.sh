#!/bin/sh
#
# Convenience launcher for the GossipRouter

SEP=":"
case "`uname`" in
    CYGWIN*)
        cygwin=true
        SEP=";"
        ;;
esac

CLASSPATH="../classes$SEP../conf$SEP../lib/commons-logging.jar$SEP../lib/log4j.jar"

if [ "$cygwin" = "true" ]; then
    CLASSPATH=`echo $CLASSPATH | sed -e 's/\;/\\\\;/g'`
fi

#-Djava.util.logging.config.file=./java.logging.config

echo "$CLASSPATH args=$*"
java -classpath $CLASSPATH $JAVA_OPTS org.jgroups.stack.GossipRouter $*

