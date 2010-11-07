@rem Convenience launcher for the Draw demo (contributed by Laran Evans lc278@cornell.edu)
@echo off

set CPATH=../classes;../conf;

set JAVA_OPTS=
if -debug==%1 set JAVA_OPTS=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=jgc1

set PROPS=TUNNEL(router_host=localhost;router_port=5556)
set PROPS=%PROPS%:TCPGOSSIP(initial_hosts=localhost[5556];gossip_refresh_rate=10000;num_initial_members=3;up_thread=true;down_thread=true)
set PROPS=%PROPS%:MERGE2(min_interval=5000;max_interval=10000)
set PROPS=%PROPS%:FD_SOCK
set PROPS=%PROPS%:VERIFY_SUSPECT(timeout=1500)
set PROPS=%PROPS%:pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800)
set PROPS=%PROPS%:UNICAST(timeout=600,1200,2400,4800)
set PROPS=%PROPS%:pbcast.STABLE(desired_avg_gossip=20000)
set PROPS=%PROPS%:FRAG(frag_size=8096;down_thread=false;up_thread=false)
set PROPS=%PROPS%:pbcast.GMS(join_timeout=5000;print_local_addr=true)

@echo on
java -classpath %CPATH% %JAVA_OPTS% org.jgroups.demos.Draw -props %PROPS%
