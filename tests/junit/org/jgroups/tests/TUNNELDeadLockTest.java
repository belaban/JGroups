package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.Promise;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test designed to make sure the TUNNEL doesn't lock the client and the GossipRouter
 * under heavy load.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @version $Id: TUNNELDeadLockTest.java,v 1.20 2009/11/23 20:12:55 vlada Exp $
 * @see TUNNELDeadLockTest#testStress
 */
@Test(groups={Global.STACK_INDEPENDENT, Global.GOSSIP_ROUTER},sequential=true)
public class TUNNELDeadLockTest extends ChannelTestBase {
    private JChannel channel;
    private Promise<Boolean> promise;
    private int receivedCnt;

    // the total number of the messages pumped down the channel
    private int msgCount=20000;
    // the message payload size (in bytes);
    private int payloadSize=32;
    // the time (in ms) the main thread waits for all the messages to arrive,
    // before declaring the test failed.
    private int mainTimeout=60000;
    GossipRouter gossipRouter;


    @BeforeMethod
    void setUp() throws Exception {
        String bind_addr=Util.getProperty(Global.BIND_ADDR);
        if(bind_addr == null) {
            StackType type=Util.getIpStackType();
            if(type == StackType.IPv6)
                bind_addr="::1";
            else
                bind_addr="127.0.0.1";
        }
        promise=new Promise<Boolean>();
        gossipRouter=new GossipRouter(GossipRouter.PORT,bind_addr);
        gossipRouter.start();
    }

    @AfterMethod(alwaysRun=true)
    void tearDown() throws Exception {
        // I prefer to close down the channel inside the test itself, for the
        // reason that the channel might be brought in an uncloseable state by
        // the test.

        // TO_DO: no elegant way to stop the Router threads and clean-up
        //        resources. Use the Router administrative interface, when available.
        
        channel.close();
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
        channel=new JChannel("tunnel.xml");
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
                        channel.send(null, null, new byte[payloadSize]);
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
            String msg=
                    "The channel has failed to send/receive " + msgCount + " messages " +
                    "possibly because of the channel deadlock or too short " +
                    "timeout (currently " + mainTimeout + " ms). " + receivedCnt +
                    " messages received so far.";
            assert false : msg;
        }       
    }
}
