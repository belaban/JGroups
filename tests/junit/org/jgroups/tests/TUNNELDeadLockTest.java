package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.Promise;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;

/**
 * Test designed to make sure the TUNNEL doesn't lock the client and the GossipRouter
 * under heavy load.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @see TUNNELDeadLockTest#testStress
 */
@Test(groups={Global.STACK_INDEPENDENT, Global.GOSSIP_ROUTER, Global.EAP_EXCLUDED},singleThreaded=true)
public class TUNNELDeadLockTest extends ChannelTestBase {
    private JChannel         channel;
    private Promise<Boolean> promise;
    private int              receivedCnt;

    // the total number of the messages pumped down the channel
    private static final int msgCount=20000;
    // the message payload size (in bytes);
    private static final int payloadSize=32;
    // the time (in ms) the main thread waits for all the messages to arrive,
    // before declaring the test failed.
    private static final int mainTimeout=60000;
    GossipRouter             gossipRouter;
    private int              gossip_router_port;
    private String           gossip_router_hosts;


    @BeforeMethod
    void setUp() throws Exception {
        StackType type=Util.getIpStackType();
        if(type == StackType.IPv6)
            bind_addr="::1";
        else
            bind_addr="127.0.0.1";
        promise=new Promise<Boolean>();
        gossip_router_port=ResourceManager.getNextTcpPort(InetAddress.getByName(bind_addr));
        gossip_router_hosts=bind_addr + "[" + gossip_router_port + "]";
        gossipRouter=new GossipRouter(gossip_router_port, bind_addr);
        gossipRouter.start();
    }

    @AfterMethod(alwaysRun=true)
    void tearDown() throws Exception {
        // I prefer to close down the channel inside the test itself, for the
        // reason that the channel might be brought in an uncloseable state by
        // the test.

        // TO_DO: no elegant way to stop the Router threads and clean-up
        //        resources. Use the Router administrative interface, when available.
        
        Util.close(channel);
        promise.reset();
        promise=null;
        gossipRouter.stop();
        System.out.println("Router stopped");
    }




    /**
     * Pushes messages down the channel as fast as possible. Sometimes this
     * manages to bring the channel and the Router into deadlock. On the
     * machine I run it usually happens after 700 - 1000 messages and I
     * suspect that this number it is related to the socket buffer size.
     * (the comments are written when I didn't solve the bug yet). <br>
     * <p/>
     * The number of messages sent can be controlled with msgCount.
     * The time (in ms) the main threads wait for the all messages to come can
     * be controlled with mainTimeout. If this time passes and the test
     * doesn't see all the messages, it declares itself failed.
     */
    @Test
    public void testStress() throws Exception {
        channel=createTunnelChannel("A");
        channel.connect("agroup");
        channel.setReceiver(new ReceiverAdapter() {

            @Override
            public void receive(Message msg) {
                receivedCnt++;
                if(receivedCnt % 2000 == 0)
                    System.out.println("-- received " + receivedCnt);
                if(receivedCnt == msgCount) {
                    // let the main thread know I got all msgs
                    promise.setResult(Boolean.TRUE);
                }
            }            
        });
      
        // stress send messages - the sender thread
        new Thread(new Runnable() {
            public void run() {
                try {
                    for(int i=0; i < msgCount; i++) {
                        channel.send(null, new byte[payloadSize]);
                        if(i % 2000 == 0)
                            System.out.println("-- sent " + i);
                    }
                }
                catch(Exception e) {
                    System.err.println("Error sending data over ...");
                    e.printStackTrace();
                }
            }
        }).start();


        // wait for all the messages to come; if I don't see all of them in
        // mainTimeout ms, I fail the test

        Boolean result=promise.getResult(mainTimeout);
        if(result == null) {
            String msg="The channel has failed to send/receive " + msgCount + " messages " +
                    "possibly because of the channel deadlock or too short " +
                    "timeout (currently " + mainTimeout + " ms). " + receivedCnt +
                    " messages received so far.";
            assert false : msg;
        }       
    }

    protected JChannel createTunnelChannel(String name) throws Exception {
        TUNNEL tunnel=(TUNNEL)new TUNNEL().setValue("bind_addr", InetAddress.getByName(bind_addr));
        tunnel.setGossipRouterHosts(gossip_router_hosts);
        JChannel ch=Util.createChannel(tunnel,
                                       new PING(),
                                       new MERGE3().setValue("min_interval", 1000).setValue("max_interval", 3000),
                                       new FD().setValue("timeout", 2000).setValue("max_tries", 2),
                                       new VERIFY_SUSPECT(),
                                       new NAKACK2().setValue("use_mcast_xmit", false),
                                       new UNICAST3(), new STABLE(), new GMS().joinTimeout(1000));
        if(name != null)
            ch.setName(name);
        return ch;
    }
}
