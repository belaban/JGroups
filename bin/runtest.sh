#!/bin/sh

# Runs a single test class from command line, circumventing ant altogether. Useful for
# quick debugging.

TESTCLASS=org.jgroups.tests.stack.RouterTest

reldir=`dirname $0`

# OS specific support (must be 'true' or 'false').
cygwin=false;
case "`uname`" in
    CYGWIN*)
        cygwin=true
        ;;
esac

if [ $cygwin = true ]; then
    SEP=";"
else
    SEP=":"
fi

while [ "$1" != "" ]; do
    if [ "$1" = "-debug" ]; then
        if [ $cygwin = false ]; then
            JAVA_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=12348"
        else
            JAVA_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=jgroups"
        fi
    fi
    shift
done


CLASSPATH="$reldir/../classes${SEP}\
$reldir/../conf${SEP}\
$reldir/../lib/junit.jar${SEP}\
$reldir/../lib/log4j.jar${SEP}

#if [ $cygwin = "true" ]; then
#   CP=`cygpath -wp $CLASSPATH`
#else
#   CP=$CLASSPATH
#fi

#echo $CLASSPATH
java $JAVA_OPTS -cp $CLASSPATH junit.textui.TestRunner $TESTCLASS
