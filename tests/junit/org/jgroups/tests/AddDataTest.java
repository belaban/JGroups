package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.ChannelException;
import org.jgroups.Event;
import org.jgroups.JChannel;
import org.jgroups.stack.IpAddress;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author Bela Ban
 * @version $Id: AddDataTest.java,v 1.5 2004/03/30 06:47:31 belaban Exp $
 */
public class AddDataTest extends TestCase {

    
    String props="UDP(mcast_addr=228.1.2.3;mcast_port=45566;ip_ttl=32):" +
        "PING(timeout=2000;num_initial_members=2):" +
        "FD(timeout=1000;max_tries=2):" +
        "VERIFY_SUSPECT(timeout=1500):" +
        "pbcast.NAKACK(gc_lag=10;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400,4800):" +
        "pbcast.STABLE(desired_avg_gossip=10000):" +
        "FRAG:" +
        "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
        "shun=true;print_local_addr=true)";



    public AddDataTest(String name) {
        super(name);

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
            for(int i=1; i <= 10; i++) {
                System.out.println("-- attempt # " + i + "/10");
                JChannel c=new JChannel(props);
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


    public static void main(String[] args) {
        String[] testCaseName={AddDataTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
