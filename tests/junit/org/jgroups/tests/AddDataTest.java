package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.stack.IpAddress;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Bela Ban
 * @version $Id: AddDataTest.java,v 1.7 2005/04/19 12:11:41 belaban Exp $
 */
public class AddDataTest extends TestCase {
    JChannel ch1, ch2;
    
    String properties="UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32;down_thread=false;up_thread=false):" +
            "PING(timeout=2000;num_initial_members=2;down_thread=false;up_thread=false):" +
            "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800;down_thread=false;up_thread=false):" +
            "UNICAST(timeout=600,1200,2400,4800;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=true;down_thread=false;up_thread=false)";

    String bundlingProperties="UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32;" +
            "enable_bundling=true;max_bundle_size=3000;max_bundle_timeout=500;down_thread=false;up_thread=false):" +
            "PING(timeout=2000;num_initial_members=2;down_thread=false;up_thread=false):" +
            "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800;down_thread=false;up_thread=false):" +
            "UNICAST(timeout=600,1200,2400,4800;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=true;down_thread=false;up_thread=false)";




    public AddDataTest(String name) {
        super(name);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if(ch2 != null)
            ch2.close();
        if(ch1 != null)
            ch1.close();
    }


    /**
     * Uncomment to test shunning/reconnecting (using CTRL-Z and fg)
     */
//    public void testAdditionalDataWithShun() {
//        try {
//            JChannel c=new JChannel(props);
//            Map m=new HashMap();
//            m.put("additional_data", new byte[]{'b', 'e', 'l', 'a'});
//            c.down(new Event(Event.CONFIG, m));
//            c.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
//            c.setChannelListener(new ChannelListener() {
//                public void channelDisconnected(Channel channel) {
//                    System.out.println("channel disconnected");
//                }
//
//                public void channelShunned() {
//                    System.out.println("channel shunned");
//                }
//
//                public void channelReconnected(Address addr) {
//                    System.out.println("channel reconnected");
//                }
//
//                public void channelConnected(Channel channel) {
//                    System.out.println("channel connected");
//                }
//
//                public void channelClosed(Channel channel) {
//                    System.out.println("channel closed");
//                }
//            });
//            System.out.println("CONNECTING");
//            c.connect("bla");
//            System.out.println("CONNECTING: done");
//            IpAddress addr=(IpAddress)c.getLocalAddress();
//            System.out.println("address is " + addr);
//            assertNotNull(addr.getAdditionalData());
//            assertEquals(addr.getAdditionalData()[0], 'b');
//            Util.sleep(600000);
//            c.close();
//        }
//        catch(ChannelException e) {
//            e.printStackTrace();
//            fail(e.toString());
//        }
//    }


    public void testAdditionalData() {
        try {
            for(int i=1; i <= 5; i++) {
                System.out.println("-- attempt # " + i + "/10");
                JChannel c=new JChannel(properties);
                Map m=new HashMap();
                m.put("additional_data", new byte[]{'b', 'e', 'l', 'a'});
                c.down(new Event(Event.CONFIG, m));
                c.connect("bla");
                IpAddress addr=(IpAddress)c.getLocalAddress();
                System.out.println("address is " + addr);
                assertNotNull(addr.getAdditionalData());
                assertEquals(addr.getAdditionalData()[0], 'b');
                c.close();
            }
        }
        catch(ChannelException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }


    public void testBetweenTwoChannelsMcast() throws Exception {
        _testWithProps(this.properties, true);
    }

    public void testBetweenTwoChannelsUnicast() throws Exception {
        _testWithProps(this.properties, false);
    }

    public void testBetweenTwoChannelsWithBundlingMcast() throws Exception {
        _testWithProps(this.bundlingProperties, true);
    }

    public void testBetweenTwoChannelsWithBundlingUnicast() throws Exception {
        _testWithProps(this.bundlingProperties, false);
    }



    private void _testWithProps(String props, boolean mcast) throws Exception {
        Map m=new HashMap();
        m.put("additional_data", new byte[]{'b', 'e', 'l', 'a'});
        byte[] buf=new byte[1000];

        ch1=new JChannel(props);
        ch1.down(new Event(Event.CONFIG, m));
        ch2=new JChannel(props);
        ch1.connect("group");
        ch2.connect("group");
        while(ch2.peek(10) != null) {
            System.out.println("-- received " + ch2.receive(100));
        }
        if(mcast)
            ch1.send(new Message(null, null, buf));
        else {
            Address dest=ch2.getLocalAddress();
            ch1.send(new Message(dest, null, buf));
        }
        Message msg=(Message)ch2.receive(10000);
        System.out.println("received " + msg);
        IpAddress src=(IpAddress)msg.getSrc();
        System.out.println("src=" + src);

        // Thread.sleep(600000); // todo: remove

        assertNotNull(src);
        assertNotNull(src.getAdditionalData());
        assertEquals(4, src.getAdditionalData().length);
    }


    public static void main(String[] args) {
        String[] testCaseName={AddDataTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
