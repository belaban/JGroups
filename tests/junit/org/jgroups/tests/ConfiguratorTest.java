package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.JChannel;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;

import java.util.List;
import java.util.Vector;

/**
 * Tests features of stack configuration.
 * @author Bela Ban
 * @version $Id: ConfiguratorTest.java,v 1.5.2.2 2009/04/06 16:45:22 rachmatowicz Exp $
 * 
 * NOTE: ProtocolStack.insertProtocol() and ProtocolStack.removeProtocol() are 
 * defined but not implemented in 2.4. The 2.6 version of this test was aimed at testing
 * these features. The test is used in 2.4 to focus on parsing of IP addresses.
 */
public class ConfiguratorTest extends TestCase {

    public ConfiguratorTest(String name) {
        super(name);
    }


    public void testParsing() throws Exception {
        String config="UDP(mcast_addr=ff18:eb72:479f::2:3;oob_thread_pool.max_threads=4;" +
                "oob_thread_pool.keep_alive_time=5000;max_bundle_size=64000;mcast_send_buf_size=640000;" +
                "oob_thread_pool.queue_max_size=10;mcast_recv_buf_size=25000000;" +
                "use_concurrent_stack=true;tos=8;mcast_port=45522;loopback=true;thread_pool.min_threads=2;" +
                "oob_thread_pool.rejection_policy=Run;thread_pool.max_threads=8;enable_diagnostics=true;" +
                "thread_naming_pattern=cl;ucast_send_buf_size=640000;ucast_recv_buf_size=20000000;" +
                "thread_pool.enabled=true;use_incoming_packet_handler=true;oob_thread_pool.enabled=true;ip_ttl=2;" +
                "enable_bundling=true;thread_pool.rejection_policy=Run;discard_incompatible_packets=true;" +
                "thread_pool.keep_alive_time=5000;thread_pool.queue_enabled=false;mcast_addr=228.10.10.15;" +
                "max_bundle_timeout=30;oob_thread_pool.queue_enabled=false;oob_thread_pool.min_threads=2;" +
                "thread_pool.queue_max_size=100):" +
                "PING(num_initial_members=3;timeout=2000):" +
                "MERGE2(min_interval=5000;max_interval=10000):" +
                "FD_SOCK:" +
                "FD(max_tries=3;timeout=2000):" +
                "VERIFY_SUSPECT(timeout=1500):" +
                "BARRIER:" +
                "pbcast.NAKACK(gc_lag=0;use_mcast_xmit=false;retransmit_timeout=300,600,1200,2400,4800;" +
                "discard_delivered_msgs=true):" +
                "UNICAST(loopback=false;timeout=300,600,1200,2400,3600):" +
                "pbcast.STABLE(desired_avg_gossip=50000;max_bytes=1000000;stability_delay=1000):" +
                "VIEW_SYNC(avg_send_interval=60000):" +
                "pbcast.GMS(print_local_addr=true;view_bundling=true;join_timeout=3000;" +
                "shun=false):" +
                "FC(max_block_time=10000;max_credits=5000000;min_threshold=0.25):" +
                "FRAG2(frag_size=60000):" +
                "pbcast.STREAMING_STATE_TRANSFER(use_reading_thread=true)";
        
	Configurator configurator = new Configurator() ;

        // Vector<Configurator.ProtocolConfiguration> ret=configurator.parseConfigurations(config);
        Vector ret=configurator.parseConfigurations(config);
        System.out.println("config:\n" + ret);
        assertEquals(15, ret.size());

        config="UDP(mcast_addr=ff18:eb72:479f::2:3;mcast_port=2453):pbcast.FD:FRAG(frag_size=2292):FD_SIMPLE(s=22;d=33):MERGE2(a=22)";
        ret=configurator.parseConfigurations(config);
        System.out.println("config:\n" + ret);
        assertEquals(5, ret.size());

        config="com.mycomp.Class:B:pbcast.C:H(a=b;c=d;e=f)";
        ret=configurator.parseConfigurations(config);
        System.out.println("config:\n" + ret);
        assertEquals(4, ret.size());
    }


    public static void main(String[] args) {
        String[] testCaseName={ConfiguratorTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
