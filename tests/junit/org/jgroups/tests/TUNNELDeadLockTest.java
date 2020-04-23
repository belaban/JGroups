package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.TUNNEL;
import org.jgroups.protocols.UNICAST3;
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
 * Test designed to make sure the TUNNEL doesn't lock the client and the GossipRouter under load.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @see TUNNELDeadLockTest#testStress
 */
@Test(groups={Global.STACK_INDEPENDENT, Global.GOSSIP_ROUTER, Global.EAP_EXCLUDED},singleThreaded=true)
public class TUNNELDeadLockTest {
    private JChannel         channel;
    private Promise<Boolean> promise;
    private int              receivedCnt;

    // the total number of the messages pumped down the channel
    private static final int msgCount=20_000;
    // the time (in ms) the main thread waits for all the messages to arrive,
    // before declaring the test failed.
    private static final int mainTimeout=10000;
    private String           bind_addr="loopback";
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
        promise=new Promise<>();
        gossip_router_port=ResourceManager.getNextTcpPort(InetAddress.getByName(bind_addr));
        gossip_router_hosts=bind_addr + "[" + gossip_router_port + "]";
        gossipRouter=new GossipRouter(bind_addr, gossip_router_port).useNio(false);
        gossipRouter.start();
    }

    @AfterMethod void tearDown() throws Exception {
        //TUNNEL tunnel=channel.getProtocolStack().findProtocol(TUNNEL.class);
        //System.out.printf("TUNNEL stats:\n%s\n", tunnel.getMessageStats());
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
    public void testStress() throws Exception {
        receivedCnt=0;
        channel=createTunnelChannel("A");
        channel.connect(TUNNELDeadLockTest.class.getSimpleName());
        channel.setReceiver(new Receiver() {

            @Override
            public void receive(Message msg) {
                receivedCnt++;
                if(receivedCnt % 2000 == 0)
                    System.out.println("-- received " + receivedCnt);
                if(receivedCnt >= msgCount) {
                    // let the main thread know I got all msgs
                    promise.setResult(Boolean.TRUE);
                }
            }            
        });
      
        // stress send messages - the sender thread
        new Thread(() -> {
            try {
                for(int i=1; i <= msgCount; i++) {
                    channel.send(null, i);
                    if(i % 2000 == 0)
                        System.out.println("-- sent " + i);
                }
            }
            catch(Exception e) {
                System.err.println("Error sending data over ...");
                e.printStackTrace();
            }
        }).start();

        Boolean result=promise.getResult(mainTimeout);
        assert result != null : String.format("failed to receive %d messages in %d ms (%d messages received so far)",
                                              msgCount, mainTimeout, receivedCnt);
    }

    protected JChannel createTunnelChannel(String name) throws Exception {
        TUNNEL tunnel=new TUNNEL().setBindAddress(InetAddress.getByName(bind_addr));
        tunnel.setGossipRouterHosts(gossip_router_hosts);

        JChannel ch=new JChannel(tunnel,
                                 new PING(),
                                 new NAKACK2(),
                                 new UNICAST3(),
                                 new STABLE(),
                                 new GMS().setJoinTimeout(1000)).name(name);
        if(name != null)
            ch.setName(name);
        return ch;
    }
}
