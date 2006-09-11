package org.jgroups;

/**
 * Globals used by JGroups packages.
 * 
 * @author Bela Ban Mar 29, 2004
 * @version $Id: Global.java,v 1.14 2006/09/11 14:09:52 belaban Exp $
 */
public class Global {
    /** Allows for conditional compilation; e.g., if(log.isTraceEnabled()) if(log.isInfoEnabled()) log.info(...) would be removed from the code
	(if recompiled) when this flag is set to false. Therefore, code that should be removed from the final
	product should use if(log.isTraceEnabled()) rather than .
    */
    public static final boolean debug=false;

    /**
     * Used to determine whether to copy messages (copy=true) in retransmission tables,
     * or whether to use references (copy=false). Once copy=false has worked for some time, this flag
     * will be removed entirely
     */
    public static final boolean copy=false;


    public static final String THREAD_PREFIX=" (channel=";
    
    public static final int BYTE_SIZE  = 1;
    public static final int SHORT_SIZE = 2;
    public static final int INT_SIZE   = 4;
    public static final int LONG_SIZE  = 8;

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
}
