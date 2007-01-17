package org.jgroups;

/**
 * Globals used by JGroups packages.
 * 
 * @author Bela Ban Mar 29, 2004
 * @version $Id: Global.java,v 1.19 2007/01/17 17:16:31 belaban Exp $
 */
public class Global {
    /** Allows for conditional compilation; e.g., if(log.isTraceEnabled()) if(log.isInfoEnabled()) log.info(...) would be removed from the code
	(if recompiled) when this flag is set to false. Therefore, code that should be removed from the final
	product should use if(log.isTraceEnabled()) rather than .
    */
    public static final boolean debug=false;

    public static final String THREAD_PREFIX=" (channel=";
    
    public static final int BYTE_SIZE  = Byte.SIZE    / 8; // 1
    public static final int SHORT_SIZE = Short.SIZE   / 8; // 2
    public static final int INT_SIZE   = Integer.SIZE / 8; // 4
    public static final int LONG_SIZE  = Long.SIZE    / 8; // 8

    public static final Object NULL=new Object();

    public static final String BIND_ADDR="jgroups.bind_addr";
    public static final String BIND_ADDR_OLD="bind.address";
    public static final String IGNORE_BIND_ADDRESS_PROPERTY="jgroups.ignore.bind_addr";
    public static final String IGNORE_BIND_ADDRESS_PROPERTY_OLD="ignore.bind.address";
    public static final String MARSHALLING_COMPAT="jgroups.marshalling.compatible";

    public static final String TCPPING_INITIAL_HOSTS="jgroups.tcpping.initial_hosts";

    public static final String UDP_MCAST_ADDR="jgroups.udp.mcast_addr";
    public static final String UDP_MCAST_PORT="jgroups.udp.mcast_port";
    public static final String UDP_IP_TTL="jgroups.udp.ip_ttl";

    public static final String MPING_MCAST_ADDR="jgroups.mping.mcast_addr";
    public static final String MPING_MCAST_PORT="jgroups.mping.mcast_port";
    public static final String  MPING_IP_TTL="jgroups.mping.ip_ttl";

    public static final String MAGIC_NUMBER_FILE="jgroups.conf.magic_number_file";
    public static final String RESOLVE_DNS="jgroups.resolve_dns";

    public static final String CHANNEL_LOCAL_ADDR_TIMEOUT="jgroups.channel.local_addr_timeout";

    public static final String SCHEDULER_MAX_THREADS="jgroups.scheduler.max_threads";

    public static final String TIMER_MIN_THREADS="jgroups.timer.min_threads";
    public static final String TIMER_MAX_THREADS="jgroups.timer.max_threads";
    public static final String TIMER_KEEPALIVE="jgroups.timer.keepalive";
}
