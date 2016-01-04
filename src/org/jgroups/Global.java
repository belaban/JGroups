package org.jgroups;

import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.SCOPE;

/**
 * Global is a JGroups internal class defining global variables.
 * 
 * @since 2.0
 * @author Bela Ban 
 */
public class Global {
    public static final int BYTE_SIZE   = Byte.SIZE    / 8; // 1
    public static final int SHORT_SIZE  = Short.SIZE   / 8; // 2
    public static final int INT_SIZE    = Integer.SIZE / 8; // 4
    public static final int LONG_SIZE   = Long.SIZE    / 8; // 8
    public static final int DOUBLE_SIZE = Double.SIZE  / 8; // 8;
    public static final int FLOAT_SIZE  = Float.SIZE   / 8; // 4;

    public static final int MAX_DATAGRAM_PACKET_SIZE=1 << 16;

    public static final short SCOPE_ID=ClassConfigurator.getProtocolId(SCOPE.class);

    public static final String IPv4="java.net.preferIPv4Stack";
    public static final String IPv6="java.net.preferIPv6Addresses";

    public static final String NON_LOOPBACK_ADDRESS="NON_LOOPBACK_ADDRESS";


    public static final String BIND_ADDR="jgroups.bind_addr";
    public static final String EXTERNAL_ADDR="jgroups.external_addr";
    public static final String EXTERNAL_PORT="jgroups.external_port";
    public static final String TCP_CLIENT_BIND_ADDR="jgroups.tcp.client_bind_addr";

    public static final String BIND_INTERFACE="jgroups.bind_interface";

    public static final String TCPPING_INITIAL_HOSTS="jgroups.tcpping.initial_hosts";

    public static final String UDP_MCAST_ADDR="jgroups.udp.mcast_addr";
    public static final String UDP_MCAST_PORT="jgroups.udp.mcast_port";
    public static final String UDP_IP_TTL="jgroups.udp.ip_ttl";

    public static final String MPING_MCAST_ADDR="jgroups.mping.mcast_addr";
    public static final String MPING_MCAST_PORT="jgroups.mping.mcast_port";
    public static final String MPING_IP_TTL="jgroups.mping.ip_ttl";

    public static final String BPING_BIND_PORT="jgroups.bping.bind_port";

    public static final String STOMP_BIND_ADDR="jgroups.stomp.bind_addr";
    public static final String STOMP_ENDPOINT_ADDR="jgroups.stomp.endpoint_addr";

    public static final String MAGIC_NUMBER_FILE="jgroups.conf.magic_number_file";
    public static final String PROTOCOL_ID_FILE="jgroups.conf.protocol_id_file";
    public static final String RESOLVE_DNS="jgroups.resolve_dns";
    public static final String PRINT_UUIDS="jgroups.print_uuids";
    public static final String UUID_CACHE_MAX_ELEMENTS="jgroups.uuid_cache.max_elements";
    public static final String UUID_CACHE_MAX_AGE="jgroups.uuid_cache.max_age";

    public static final String IPV6_MCAST_PREFIX="jgroups.ipmcast.prefix";

    public static final String TIMER_NUM_THREADS="jgroups.timer.num_threads";

    public static final String USE_JDK_LOGGER="jgroups.use.jdk_logger"; // forces use of the JDK logger
    public static final String CUSTOM_LOG_FACTORY="jgroups.logging.log_factory_class";
    /** System prop for defining the default number of headers in a Message */
    public static final String DEFAULT_HEADERS="jgroups.msg.default_headers";

    public static final long   DEFAULT_FIRST_UNICAST_SEQNO = 1;

    /** First ID assigned for building blocks (defined in jg-protocols.xml) */
    public static final short  BLOCKS_START_ID=200;
    
    public static final String SINGLETON_NAME="singleton_name";
    
    public static final long   THREADPOOL_SHUTDOWN_WAIT_TIME=3000;
    public static final long   THREAD_SHUTDOWN_WAIT_TIME=300;
    public static final String DUMMY="dummy-";

    public static final String MATCH_ADDR="match-address";
    public static final String MATCH_HOST="match-host";
    public static final String MATCH_INTF="match-interface";

    public static final String PREFIX="org.jgroups.protocols.";
    
    public static final String XML_VALIDATION="jgroups.xml.validation";

    // for TestNG
    public static final String FUNCTIONAL="functional";
    public static final String TIME_SENSITIVE="time-sensitive";
    public static final String STACK_DEPENDENT="stack-dependent";
    public static final String STACK_INDEPENDENT="stack-independent";
    public static final String GOSSIP_ROUTER="gossip-router";
    public static final String FLUSH="flush";
    public static final String BYTEMAN="byteman";
    public static final String EAP_EXCLUDED="eap-excluded"; // tests not supported by EAP

    public static final String INITIAL_MCAST_ADDR="INITIAL_MCAST_ADDR";
    public static final String INITIAL_MCAST_PORT="INITIAL_MCAST_PORT";
    public static final String INITIAL_TCP_PORT="INITIAL_TCP_PORT";

    public static final String CCHM_INITIAL_CAPACITY="cchm.initial_capacity";
    public static final String CCHM_LOAD_FACTOR="cchm.load_factor";
    public static final String CCHM_CONCURRENCY_LEVEL="cchm.concurrency_level";
    public static final String MAX_LIST_PRINT_SIZE="max.list.print_size";
    public static final String SUPPRESS_VIEW_SIZE="suppress.view_size";

    public static final int IPV4_SIZE=4;
    public static final int IPV6_SIZE=16;


    public static boolean getPropertyAsBoolean(String property, boolean defaultValue) {
        boolean result = defaultValue;
        try{
            String tmp = System.getProperty(property);
            if(tmp != null)
                result = Boolean.parseBoolean(tmp);
        }
        catch(Throwable t) {
        }
        return result;
    }

    public static long getPropertyAsLong(String property, long defaultValue) {
        long result = defaultValue;
        try{
            String tmp = System.getProperty(property);
            if(tmp != null)
                result = Long.parseLong(tmp);
        }
        catch(Throwable t){
        }
        return result;
    }

    public static int getPropertyAsInteger(String property, int defaultValue) {
        int result = defaultValue;
        try{
            String tmp = System.getProperty(property);
            if(tmp != null)
                result = Integer.parseInt(tmp);
        }
        catch(Throwable t){
        }
        return result;
    }
}
