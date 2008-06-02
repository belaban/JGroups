
package org.jgroups.tests.stack;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.stack.RouterStub;
import org.jgroups.util.Promise;

import java.util.List;
import java.util.Random;

/**
 * Tests routing protocol primitives with the new GossipRouter. Since 2.2.1,
 * the GossipRouter is supposed to answer Gossip requests too.
 * <p/>
 * Note: Disable DEBUG logging before this test, otherwise the stress tests
 * may timeout.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @author Bela Ban
 * @version $Id: RouterStubTest.java,v 1.3.2.1 2008/06/02 22:05:01 rachmatowicz Exp $
 * @since 2.2.1
 */
public class RouterStubTest extends TestCase {
    RouterStub stub, stub2;

    private static final Log log = LogFactory.getLog(RouterStubTest.class);
    private static final String groupName="TESTGROUP";

    private int routerPort=-1;
    private Random random=new Random();

    public RouterStubTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        routerPort=Utilities.startGossipRouter("127.0.0.1");
        stub=new RouterStub("127.0.0.1", routerPort);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        stub.disconnect();
        if(stub2 != null)
            stub2.disconnect();
        Utilities.stopGossipRouter();
    }

    /**
     * Sends a GossipRouter.GET request to a router with an empty routing table.
     */
    public void testEmptyGET() throws Exception {
        log.info("running testEmptyGET");
        List mbrs=stub.get("nosuchgroup");
        assertNotNull(mbrs);
        assertEquals(0, mbrs.size());
    }


    /**
     * Sends a GossipRouter.CONNECT request followed by a GossipRouter.GET for the
     * group just registered.
     */
    public void test_CONNECT_GET() throws Exception {
        log.info("running test_CONNECT_GET");
        stub.connect(groupName);
        Address localAddr=stub.getLocalAddress();
        System.out.println("-- my address is " + localAddr);
        assertNotNull(localAddr);
        List groupList=stub.get(groupName);
        assertEquals(1, groupList.size());
        assertEquals(localAddr, groupList.remove(0));
    }

    /**
     * Sends a GossipRouter.CONNECT request followed by a series of simple routing requests (to all
     * members of the group, to itself, to an inexistent member).
     */
    public void test_CONNECT_Route_To_Self() throws Exception {
        log.info("running test_CONNECT_Route_To_Self");
        Message msg;

        stub.connect(groupName);
        Address localAddr=stub.getLocalAddress();

        // registration is complete
        String payload="THIS IS A MESSAGE PAYLOAD " + random.nextLong();

        // send a simple routing request to all members (null dest address)
        msg=new Message(null, localAddr, payload);
        stub.send(msg, groupName);

        Message rsp=stub.receive();
        assertEquals(localAddr, rsp.getSrc());
        assertEquals(payload, rsp.getObject());

        // send a simple routing request to itself
        msg=new Message(localAddr, localAddr, payload);
        stub.send(msg, groupName);
        rsp=stub.receive();
        assertEquals(localAddr, rsp.getSrc());
        assertEquals(payload, rsp.getObject());
    }


    public void test_CONNECT_Route_To_All() throws Exception {
        log.info("running test_CONNECT_Route_To_All");
        Message msg, msgCopy;

        stub2=new RouterStub("127.0.0.1", routerPort);

        stub.connect(groupName);  // register the first member
        Address addr=stub.getLocalAddress();

        stub2.connect(groupName); // register the second member
        addr=stub2.getLocalAddress();

        // Allow the GossipClient a moment to initialize the SocketThreads
        // for each peer
        Thread.sleep(1000) ;
        
        String payload="THIS IS A MESSAGE PAYLOAD " + random.nextLong();

        // the first member sends a simple routing request to all members (null dest address)
        msg=new Message(null, addr, payload);
        stub.send(msg, groupName);

        // only the second member should receive the routing request, the router won't send a
        // message to the originator

        // the second member reads the message
        msgCopy=stub2.receive();
        assertEquals(addr, msgCopy.getSrc());
        assertNull(msgCopy.getDest());
        assertEquals(msg.getObject(), msgCopy.getObject());
        stub2.disconnect();
    }


    public void test_CONNECT_Route_To_Other() throws Exception {
        log.info("running test_CONNECT_Route_To_Other");
        Message msg, msgCopy;

        stub.connect(groupName);
        Address localAddrOne=stub.getLocalAddress();

        stub2=new RouterStub("127.0.0.1", routerPort);

        stub2.connect(groupName);
        Address localAddrTwo=stub2.getLocalAddress();
        
        // Allow the GossipClient a moment to initialize the SocketThreads
        // for each peer
        Thread.sleep(1000) ;
        
        String payload="THIS IS A MESSAGE PAYLOAD " + random.nextLong();

        // first member send a simple routing request to the second member
        msg=new Message(localAddrTwo, localAddrOne, payload);
        stub.send(msg, groupName);

        // the second member reads the message
        msgCopy=stub2.receive();
        assertEquals(localAddrOne, msgCopy.getSrc());
        assertEquals(localAddrTwo, msgCopy.getDest());
        assertEquals(msg.getObject(), msgCopy.getObject());
        stub2.disconnect();
    }





    /**
     * Sends a GossipRouter.CONNECT request followed by a series of stress routing
     * requests to all members of the group.
     */
    public void test_CONNECT_RouteStressAll() throws Exception {
        log.info("running test_CONNECT_RouteStressAll, this may take a while .... ");


        stub.connect(groupName);
        final Address localAddrOne=stub.getLocalAddress();

        stub2=new RouterStub("127.0.0.1", routerPort);
        stub2.connect(groupName);

        // Allow the GossipClient a moment to initialize the SocketThreads
        // for each peer
        Thread.sleep(1000) ;
        
        // send a series of stress routing requests to all members
        final int count=20000; // total number of messages to be sent
        int timeout=50; // nr of secs to wait for all messages to arrive

        final boolean[] received=new boolean[count];
        for(int i=0; i < count; i++) {
            received[i]=false;
        }
        final Promise waitingArea=new Promise();
        long start=System.currentTimeMillis();

        new Thread(new Runnable() {
            public void run() {
                for(int i=0; i < count; i++) {
                    Message msg=new Message(null, localAddrOne, new Integer(i));
                    try {
                        stub.send(msg, groupName);
                        if(i % 2000 == 0)
                            System.out.println("--sent " + i);
                    }
                    catch(Exception e) {
                        waitingArea.setResult(e);
                    }
                }
            }
        }, "Sending Thread").start();


        new Thread(new Runnable() {
            public void run() {
                int cnt=0;
                while(cnt < count) {
                    try {
                        Message msg=stub2.receive();
                        int index=((Integer)msg.getObject()).intValue();
                        received[index]=true;
                        cnt++;
                        if(cnt % 2000 == 0)
                            System.out.println("-- [stub2] received " + cnt);
                    }
                    catch(Exception e) {
                        waitingArea.setResult(e);
                    }
                }
                waitingArea.setResult(Boolean.TRUE);
            }
        }, "Receiving Thread stub2").start();


        new Thread(new Runnable() {
            public void run() {
                int cnt=0;
                while(cnt < count) {
                    try {
                        Message msg=stub.receive();
                        int index=((Integer)msg.getObject()).intValue();
                        received[index]=true;
                        cnt++;
                        if(cnt % 2000 == 0)
                            System.out.println("-- [stub] received " + cnt);
                    }
                    catch(Exception e) {
                        waitingArea.setResult(e);
                    }
                }
                waitingArea.setResult(Boolean.TRUE);
            }
        }, "Receiving Thread stub").start();


        // wait here the stress threads to finish
        Object result=waitingArea.getResult((long)timeout * 1000);
        long stop=System.currentTimeMillis();
        stub2.disconnect();

        int messok=0;
        for(int i=0; i < count; i++) {
            if(received[i]) {
                messok++;
            }
        }

        if(result == null) {
            fail("Timeout while waiting for all messages to be received. " +
                    messok + " messages out of " + count + " received so far.");
        }
        if(result instanceof Exception) {
            throw (Exception)result;
        }

        // make sure all messages have been received
        for(int i=0; i < count; i++) {
            if(!received[i]) {
                fail("At least message " + i + " NOT RECEIVED");
            }
        }
        System.out.println("STRESS TEST OK, " + count + " messages, " +
                1000 * count / (stop - start) + " messages/sec");
    }





    public static Test suite() {
        return new TestSuite(RouterStubTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }



}
