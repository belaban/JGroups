package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.conf.ProtocolConfiguration;
import org.jgroups.stack.Configurator;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Tests ProtocolStack.insertProtocol() and removeProtocol()
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class ConfiguratorTest {
    ProtocolStack stack;
    static final String props="UDP:PING:FD:pbcast.NAKACK(retransmit_timeouts=300,600):UNICAST:FC";
    final String[] names={"FC", "UNICAST", "NAKACK", "FD", "PING", "UDP"};
    final String[] below={"FC", "UNICAST", "TRACE", "NAKACK", "FD", "PING", "UDP"};
    final String[] above={"FC", "TRACE", "UNICAST", "NAKACK", "FD", "PING", "UDP"};



    @BeforeMethod
    void setUp() throws Exception {
        JChannel mock_channel=new JChannel() {};
        stack=new ProtocolStack(mock_channel);
    }

    
    public void testRemovalOfTop() throws Exception {
        stack.setup(Configurator.parseConfigurations(props));
        Protocol prot=stack.removeProtocol("FC");
        assert prot != null;
        List<Protocol> protocols=stack.getProtocols();
        Assert.assertEquals(5, protocols.size());
        assert protocols.get(0).getName().endsWith("UNICAST");
        assert  stack.getTopProtocol().getUpProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol().getUpProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol().getDownProtocol() != null;
    }
    
    public void testRemovalOfBottom() throws Exception {
        stack.setup(Configurator.parseConfigurations(props));
        Protocol prot=stack.removeProtocol("UDP");
        assert prot != null;
        List<Protocol> protocols=stack.getProtocols();
        Assert.assertEquals(5, protocols.size());
        assert protocols.get(protocols.size() -1).getName().endsWith("PING");
    }
    
    public void testAddingAboveTop() throws Exception{
        stack.setup(Configurator.parseConfigurations(props));
        Protocol new_prot=(Protocol)Class.forName("org.jgroups.protocols.TRACE").newInstance();
        stack.insertProtocol(new_prot, ProtocolStack.ABOVE, "FC");
        List<Protocol> protocols=stack.getProtocols();
        Assert.assertEquals(7, protocols.size());       
        assert protocols.get(0).getName().endsWith("TRACE");
        assert  stack.getTopProtocol().getUpProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol().getUpProtocol() != null;
        assert  stack.getTopProtocol().getDownProtocol().getDownProtocol() != null;
    }
    
    @Test(expectedExceptions={IllegalArgumentException.class})
    public void testAddingBelowBottom() throws Exception{
        stack.setup(Configurator.parseConfigurations(props));           
        Protocol new_prot=(Protocol)Class.forName("org.jgroups.protocols.TRACE").newInstance();
        stack.insertProtocol(new_prot, ProtocolStack.BELOW, "UDP");        
    }
    
    

    public void testInsertion() throws Exception {
        stack.setup(Configurator.parseConfigurations(props));
        List<Protocol> protocols=stack.getProtocols();
        assert protocols != null;
        Assert.assertEquals(6, protocols.size());

        for(int i=0; i < names.length; i++) {
            String name=names[i];
            Protocol p=protocols.get(i);
            Assert.assertEquals(name, p.getName());
        }

        // insert below
        Protocol new_prot=(Protocol)Class.forName("org.jgroups.protocols.TRACE").newInstance();
        stack.insertProtocol(new_prot, ProtocolStack.BELOW, "UNICAST");
        protocols=stack.getProtocols();
        Assert.assertEquals(7, protocols.size());
        for(int i=0; i < below.length; i++) {
            String name=below[i];
            Protocol p=protocols.get(i);
            Assert.assertEquals(name, p.getName());
        }

        // remove
        Protocol prot=stack.removeProtocol("TRACE");
        assert prot != null;
        protocols=stack.getProtocols();
        Assert.assertEquals(6, protocols.size());
        for(int i=0; i < names.length; i++) {
            String name=names[i];
            Protocol p=protocols.get(i);
            Assert.assertEquals(name, p.getName());
        }

        // insert above
        new_prot=(Protocol)Class.forName("org.jgroups.protocols.TRACE").newInstance();
        stack.insertProtocol(new_prot, ProtocolStack.ABOVE, "UNICAST");
        protocols=stack.getProtocols();
        Assert.assertEquals(7, protocols.size());
        for(int i=0; i < above.length; i++) {
            String name=above[i];
            Protocol p=protocols.get(i);
            Assert.assertEquals(name, p.getName());
        }
    }


    public static void testParsing() throws Exception {
        String config="UDP(mcast_addr=ff18:eb72:479f::2:3;oob_thread_pool.max_threads=4;" +
                "oob_thread_pool.keep_alive_time=5000;max_bundle_size=64000;mcast_send_buf_size=640000;" +
                "oob_thread_pool.queue_max_size=10;mcast_recv_buf_size=25000000;" +
                "tos=8;mcast_port=45522;loopback=true;thread_pool.min_threads=2;" +
                "oob_thread_pool.rejection_policy=Run;thread_pool.max_threads=8;enable_diagnostics=true;" +
                "thread_naming_pattern=cl;ucast_send_buf_size=640000;ucast_recv_buf_size=20000000;" +
                "thread_pool.enabled=true;oob_thread_pool.enabled=true;ip_ttl=2;" +
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
                "pbcast.GMS(print_local_addr=true;view_bundling=true;join_timeout=3000):" +
                "FC(max_block_time=10000;max_credits=5000000;min_threshold=0.25):" +
                "FRAG2(frag_size=60000):" +
                "pbcast.STREAMING_STATE_TRANSFER(use_reading_thread=true)";
        
        List<ProtocolConfiguration> ret=Configurator.parseConfigurations(config);
        System.out.println("config:\n" + ret);
        Assert.assertEquals(14, ret.size());

        config="UDP(mcast_addr=ff18:eb72:479f::2:3;mcast_port=2453):pbcast.FD:FRAG(frag_size=2292):FD_SIMPLE(s=22;d=33):MERGE2(a=22)";
        ret=Configurator.parseConfigurations(config);
        System.out.println("config:\n" + ret);
        Assert.assertEquals(5, ret.size());

        config="com.mycomp.Class:B:pbcast.C:H(a=b;c=d;e=f)";
        ret=Configurator.parseConfigurations(config);
        System.out.println("config:\n" + ret);
        Assert.assertEquals(4, ret.size());
    }





}
