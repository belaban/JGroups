// $Id: TUNNELDeadLockTest.java,v 1.10 2006/10/11 14:32:38 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.TimeoutException;
import org.jgroups.tests.stack.Utilities;
import org.jgroups.util.Promise;

/**
 * Test designed to make sure the TUNNEL doesn't lock the client and the GossipRouter
 * under heavy load.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @version $Revision: 1.10 $
 * @see TUNNELDeadLockTest#testStress
 */
public class TUNNELDeadLockTest extends TestCase {
    private JChannel channel;
    private Promise promise;
    private int receivedCnt;

    // the total number of the messages pumped down the channel
    private int msgCount=20000;
    // the message payload size (in bytes);
    private int payloadSize=32;
    // the time (in ms) the main thread waits for all the messages to arrive,
    // before declaring the test failed.
    private int mainTimeout=60000;

    int routerPort=-1;


    public TUNNELDeadLockTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        promise=new Promise();
        routerPort=Utilities.startGossipRouter("127.0.0.1");
    }

    public void tearDown() throws Exception {

        super.tearDown();

        // I prefer to close down the channel inside the test itself, for the
        // reason that the channel might be brought in an uncloseable state by
        // the test.

        // TO_DO: no elegant way to stop the Router threads and clean-up
        //        resources. Use the Router administrative interface, when
        //        available.

        channel=null;
        promise.reset();
        promise=null;
        Utilities.stopGossipRouter();
    }


    private String getTUNNELProps(int routerPort) {
        String props;

        props="TUNNEL(router_host=127.0.0.1;router_port=" + routerPort + "):" +
                "PING(timeout=3000;gossip_refresh=10000;num_initial_members=3;" +
                "gossip_host=127.0.0.1;gossip_port=" + routerPort + "):" +
                "FD_SOCK:" +
                "pbcast.NAKACK(gc_lag=100;retransmit_timeout=600,1200,2400,4800):" +
                "pbcast.STABLE(stability_delay=1000;desired_avg_gossip=20000;down_thread=false;max_bytes=0;up_thread=false):" +
                "pbcast.GMS(print_local_addr=true;join_timeout=5000;join_retry_timeout=2000;shun=true)";
        return props;
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
        String props=getTUNNELProps(routerPort);
        channel=new JChannel(props);
        channel.connect("agroup");

        // receiver thread
        new Thread(new Runnable() {
            public void run() {
                try {
                    while(true) {
                        if(channel == null)
                            return;
                        Object o=channel.receive(10000);
                        if(o instanceof Message) {
                            receivedCnt++;
                            if(receivedCnt % 2000 == 0)
                                System.out.println("-- received " + receivedCnt);
                            if(receivedCnt == msgCount) {
                                // let the main thread know I got all msgs
                                promise.setResult(new Object());
                                return;
                            }
                        }
                    }
                }
                catch(TimeoutException e) {
                    System.err.println("Timeout receiving from the channel. " + receivedCnt +
                            " msgs received so far.");
                }
                catch(Exception e) {
                    System.err.println("Error receiving data");
                    e.printStackTrace();
                }
            }
        }).start();

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

        Object result=promise.getResult(mainTimeout);
        if(result == null) {
            String msg=
                    "The channel has failed to send/receive " + msgCount + " messages " +
                    "possibly because of the channel deadlock or too short " +
                    "timeout (currently " + mainTimeout + " ms). " + receivedCnt +
                    " messages received so far.";
            fail(msg);
        }

        // don't close it in tearDown() because it hangs forever for a failed
        // test.
        channel.close();
    }


    public static Test suite() {
        return new TestSuite(TUNNELDeadLockTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }


}
