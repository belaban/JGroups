#!/bin/sh
#
# Convenience launcher for the Draw demo
#

SEP=":"
case "`uname`" in
    CYGWIN*)
        cygwin=true
        SEP=";"
        ;;

    Darwin*)
        darwin=true
        ;;
esac
relpath=`dirname $0`

while [ "$1" != "" ]; do
    if [ "$1" = "-debug" ]; then
        debug=true
    fi
    shift
done


CLASSPATH="$relpath/../classes$SEP$relpath/../conf$SEP"

if [ "$debug" = "true" ]; then
    JAVA_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=jgc1"
fi


if [ "$cygwin" = "true" ]; then
    CLASSPATH=`echo $CLASSPATH | sed -e 's/\;/\\\\;/g'`
fi


java -classpath $CLASSPATH $JAVA_OPTS org.jgroups.demos.Draw $*

