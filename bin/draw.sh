#!/bin/sh
#
# Convenience launcher for the Draw demo
#

case "`uname`" in
    CYGWIN*)
        cygwin=true
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

CLASSPATH="$relpath/../classes;$relpath/../conf;$relpath/../lib/commons-logging.jar;$relpath/../lib/log4j-1.2.6.jar"

if [ "$debug" = "true" ]; then
    JAVA_OPTS="-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=jgc1"
fi
PROPS="\
TUNNEL(router_host=localhost;router_port=5556):\
TCPGOSSIP(initial_hosts=localhost[5556];gossip_refresh_rate=10000;num_initial_members=3;up_thread=true;down_thread=true):\
MERGE2(min_interval=5000;max_interval=10000):\
FD_SOCK:\
VERIFY_SUSPECT(timeout=1500):\
pbcast.NAKACK(max_xmit_size=8096;gc_lag=50;retransmit_timeout=600,1200,2400,4800):\
UNICAST(timeout=600,1200,2400,4800):\
pbcast.STABLE(desired_avg_gossip=20000):\
FRAG(frag_size=8096;down_thread=false;up_thread=false):\
pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;print_local_addr=true)"


if [ "$cygwin" = "true" ]; then
    CLASSPATH=`echo $CLASSPATH | sed -e 's/\;/\\\\;/g'`
fi


java -classpath $CLASSPATH $JAVA_OPTS org.jgroups.demos.Draw -props $PROPS

