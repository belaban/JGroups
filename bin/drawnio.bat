@rem Convenience launcher for the Draw demo (contributed by Laran Evans lc278@cornell.edu)
@echo off

set CPATH=../classes;../conf;../lib/commons-logging.jar;../lib/log4j-1.2.6.jar;../lib/concurrent.jar;../conf/log4j.properties

set JAVA_OPTS=
if -debug==%1 set JAVA_OPTS=-Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_shmem,server=y,suspend=y,address=jgc1

set PROPS=TCP_NIO(
set PROPS=%PROPS%recv_buf_size=20000000;
set PROPS=%PROPS%send_buf_size=640000;
set PROPS=%PROPS%loopback=false;
set PROPS=%PROPS%discard_incompatible_packets=true;
set PROPS=%PROPS%max_bundle_size=64000;
set PROPS=%PROPS%max_bundle_timeout=30;
set PROPS=%PROPS%use_incoming_packet_handler=true;
set PROPS=%PROPS%use_outgoing_packet_handler=false;
set PROPS=%PROPS%enable_bundling=true;
set PROPS=%PROPS%start_port=7800;
set PROPS=%PROPS%sock_conn_timeout=300;skip_suspected_members=true;
set PROPS=%PROPS%reader_threads=8;
set PROPS=%PROPS%writer_threads=8;
set PROPS=%PROPS%processor_threads=8;
set PROPS=%PROPS%processor_minThreads=8;
set PROPS=%PROPS%processor_maxThreads=8;
set PROPS=%PROPS%use_send_queues=false)
set PROPS=%PROPS%TCPPING(timeout=3000;
set PROPS=%PROPS%initial_hosts=localhost[7800],localhost[7801];
set PROPS=%PROPS%port_range=1;
set PROPS=%PROPS%num_initial_members=3)
set PROPS=%PROPS%MERGE2(max_interval=100000;
set PROPS=%PROPS%min_interval=20000)
set PROPS=%PROPS%FD_SOCK
set PROPS=%PROPS%FD(timeout=10000;max_tries=5;shun=true)
set PROPS=%PROPS%VERIFY_SUSPECT(timeout=1500)
set PROPS=%PROPS%pbcast.NAKACK(max_xmit_size=60000;
set PROPS=%PROPS%use_mcast_xmit=false;gc_lag=0;
set PROPS=%PROPS%retransmit_timeout=300,600,1200,2400,4800;
set PROPS=%PROPS%discard_delivered_msgs=true)
set PROPS=%PROPS%pbcast.STABLE(stability_delay=1000;desired_avg_gossip=50000;
set PROPS=%PROPS%max_bytes=400000)
set PROPS=%PROPS%pbcast.GMS(print_local_addr=true;join_timeout=3000;
set PROPS=%PROPS%join_retry_timeout=2000;shun=true;
set PROPS=%PROPS%view_bundling=true)
set PROPS=%PROPS%FC(max_credits=2000000;
set PROPS=%PROPS%min_threshold=0.10)
set PROPS=%PROPS%FRAG2(frag_size=60000)
set PROPS=%PROPS%pbcast.STREAMING_STATE_TRANSFER(use_flush=true;use_reading_thread=true)
set PROPS=%PROPS%pbcast.FLUSH
    
@echo on
java -classpath %CPATH% %JAVA_OPTS% org.jgroups.demos.Draw -props %PROPS%
