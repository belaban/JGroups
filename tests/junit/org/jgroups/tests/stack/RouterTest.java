// $Id: RouterTest.java,v 1.3 2004/01/16 07:48:15 belaban Exp $

package org.jgroups.tests.stack;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.util.Random;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.log.Trace;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.GossipRouter;
import org.jgroups.util.List;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;

/**
 * Tests routing protocol primitives with the new GossipRouter. Since 2.2.1, 
 * the GossipRouter is supposed to answer Gossip requests too. 
 * 
 * Note: Disable DEBUG logging before this test, otherwise the stress tests
 *       may timeout.
 *
 * @since 2.2.1
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @version $Revision: 1.3 $
 **/
public class RouterTest extends TestCase {

    private int routerPort = -1;
    private Random random = new Random();
    
    public RouterTest(String name) {
	super(name);
        Trace.setIdentifier("org.jgroups");
    }
    
    public void setUp() throws Exception {
	super.setUp();
        routerPort = Utilities.startGossipRouter();
    }

    public void tearDown() throws Exception {
	super.tearDown();
        Utilities.stopGossipRouter();
    }

    /**
     * Sends a GossipRouter.GET request to a router with an empty routing table.
     **/
    public void testEmptyGET() throws Exception {
        int len;
        byte[] buffer;

        Socket s = new Socket("localhost", routerPort);
        DataInputStream dis = new DataInputStream(s.getInputStream());
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        // read the IpAddress sent by GossipRouter
        len = dis.readInt();
        buffer = new byte[len];
        dis.readFully(buffer, 0, len);
        IpAddress localAddr = (IpAddress)Util.objectFromByteBuffer(buffer);
        assertEquals(localAddr.getIpAddress(), s.getLocalAddress());
        assertEquals(localAddr.getPort(), s.getLocalPort());

        // send GET request
        dos.writeInt(GossipRouter.GET);
        dos.writeUTF("nosuchgroup");

        // read the answer
        len = dis.readInt();
        assertEquals(0, len);

        // check for end of stream
        assertEquals(-1, dis.read());

        dis.close();
        dos.close();
        s.close();
    }


    
    /**
     * Sends a GossipRouter.REGISTER request followed by a GossipRouter.GET for the
     * group just registered.
     **/
    public void test_REGISTER_GET() throws Exception {

        int len;
        byte[] buffer;
        String groupName = "TESTGROUP";

        Socket s = new Socket("localhost", routerPort);
        DataInputStream dis = new DataInputStream(s.getInputStream());
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        // read the IpAddress sent by GossipRouter
        len = dis.readInt();
        buffer = new byte[len];
        dis.readFully(buffer, 0, len);
        IpAddress localAddr = (IpAddress)Util.objectFromByteBuffer(buffer);
        assertEquals(localAddr.getIpAddress(), s.getLocalAddress());
        assertEquals(localAddr.getPort(), s.getLocalPort());

        // send REGISTER request
        dos.writeInt(GossipRouter.REGISTER);
        dos.writeUTF(groupName);

        // send the Address back to the router
        buffer = Util.objectToByteBuffer(localAddr);
        dos.writeInt(buffer.length);
        dos.write(buffer, 0, buffer.length);
        dos.flush();

        // registration is complete, send a GET request
        Socket s2 = new Socket("localhost", routerPort);
        DataInputStream dis2 = new DataInputStream(s2.getInputStream());
        DataOutputStream dos2 = new DataOutputStream(s2.getOutputStream());

        // read the IpAddress sent by GossipRouter
        len = dis2.readInt();
        buffer = new byte[len];
        dis2.readFully(buffer, 0, len);
        IpAddress localAddr2 = (IpAddress)Util.objectFromByteBuffer(buffer);
        assertEquals(localAddr2.getIpAddress(), s2.getLocalAddress());
        assertEquals(localAddr2.getPort(), s2.getLocalPort());

        // send GET request
        dos2.writeInt(GossipRouter.GET);
        dos2.writeUTF(groupName);

        // read the answer
        len = dis2.readInt();
        buffer = new byte[len];
        dis2.readFully(buffer, 0, len);

        List groupList = (List)Util.objectFromByteBuffer(buffer);
        assertEquals(1, groupList.size());
        assertEquals(localAddr, groupList.removeFromHead());

        // check for end of stream
        assertEquals(-1, dis2.read());

        // close the GET connection
        dis2.close();
        dos2.close();
        s2.close();

        // close the routing connection
        dis.close();
        dos.close();
        s.close();
    }

    /**
     * Sends a GossipRouter.REGISTER request followed by a series of simple routing
     * requests (to all members of the group, to itself, to an inexistent 
     * member).
     **/

    public void test_REGISTER_Route() throws Exception {

        int len;
        byte[] buffer, destAddrBuffer;
        String groupName = "TESTGROUP";
        Message msg, msgCopy;

        Socket s = new Socket("localhost", routerPort);
        DataInputStream dis = new DataInputStream(s.getInputStream());
        DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        // read the IpAddress sent by GossipRouter
        len = dis.readInt();
        buffer = new byte[len];
        dis.readFully(buffer, 0, len);
        IpAddress localAddr = (IpAddress)Util.objectFromByteBuffer(buffer);
        assertEquals(localAddr.getIpAddress(), s.getLocalAddress());
        assertEquals(localAddr.getPort(), s.getLocalPort());

        // send REGISTER request
        dos.writeInt(GossipRouter.REGISTER);
        dos.writeUTF(groupName);

        // send the Address back to the router
        buffer = Util.objectToByteBuffer(localAddr);
        dos.writeInt(buffer.length);
        dos.write(buffer, 0, buffer.length);
        dos.flush();

        // registration is complete

        String payload = "THIS IS A MESSAGE PAYLOAD "+random.nextLong();

        // send a simple routing request to all members (null dest address)
        msg = new Message(null, localAddr, payload);
        buffer = Util.objectToByteBuffer(msg);
        dos.writeUTF(groupName); 
        dos.writeInt(0);
        dos.writeInt(buffer.length);
        dos.write(buffer, 0, buffer.length);
        
        // read the message
        len = dis.readInt();
        buffer = new byte[len];
        dis.readFully(buffer, 0, len);
        msgCopy = (Message)Util.objectFromByteBuffer(buffer);
        assertEquals(msg.getSrc(), msgCopy.getSrc());
        assertNull(msgCopy.getDest());
        assertEquals(msg.getObject(), msgCopy.getObject());

        // send a simple routing request to itself
        msg = new Message(localAddr, localAddr, payload);
        buffer = Util.objectToByteBuffer(msg);
        dos.writeUTF(groupName); 
        destAddrBuffer = Util.objectToByteBuffer(localAddr);
        dos.writeInt(destAddrBuffer.length);
        dos.write(destAddrBuffer, 0, destAddrBuffer.length);
        dos.writeInt(buffer.length);
        dos.write(buffer, 0, buffer.length);
        
        // read the message
        len = dis.readInt();
        buffer = new byte[len];
        dis.readFully(buffer, 0, len);
        msgCopy = (Message)Util.objectFromByteBuffer(buffer);
        assertEquals(msg.getSrc(), msgCopy.getSrc());
        assertEquals(msg.getDest(), msgCopy.getDest());
        assertEquals(msg.getObject(), msgCopy.getObject());

        // send a simple routing request to an inexistent member, the message
        // should be discarded by router
        Address inexistentAddress =
            new IpAddress("localhost",Utilities.getFreePort());

        msg = new Message(inexistentAddress, localAddr, payload);
        buffer = Util.objectToByteBuffer(msg);
        dos.writeUTF(groupName); 
        destAddrBuffer = Util.objectToByteBuffer(inexistentAddress);
        dos.writeInt(destAddrBuffer.length);
        dos.write(destAddrBuffer, 0, destAddrBuffer.length);
        dos.writeInt(buffer.length);
        dos.write(buffer, 0, buffer.length);
        
        // the message should be discarded by router
        
        // close the routing connection
        dis.close();
        dos.close();
        s.close();
    }


    /**
     * Sends a GossipRouter.REGISTER request followed by a series of stress routing
     * requests to all members of the group.
     **/
    public void test_REGISTER_RouteStressAll() throws Exception {

        int len;
        byte[] buffer, destAddrBuffer;
        final String groupName = "TESTGROUP";
        Message msg, msgCopy;

        Socket s = new Socket("localhost", routerPort);
        final DataInputStream dis = new DataInputStream(s.getInputStream());
        final DataOutputStream dos = new DataOutputStream(s.getOutputStream());

        // read the IpAddress sent by GossipRouter
        len = dis.readInt();
        buffer = new byte[len];
        dis.readFully(buffer, 0, len);
        final IpAddress localAddr =
            (IpAddress)Util.objectFromByteBuffer(buffer);
        assertEquals(localAddr.getIpAddress(), s.getLocalAddress());
        assertEquals(localAddr.getPort(), s.getLocalPort());

        // send REGISTER request
        dos.writeInt(GossipRouter.REGISTER);
        dos.writeUTF(groupName);

        // send the Address back to the router
        buffer = Util.objectToByteBuffer(localAddr);
        dos.writeInt(buffer.length);
        dos.write(buffer, 0, buffer.length);
        dos.flush();

        // registration is complete

        // send a series of stress routing requests to all members 
        final int count = 100000; // total number of messages to be sent
        int timeout = 120; // nr of secs to wait for all messages to arrrive

        final boolean[] received = new boolean[count];
        for(int i=0; i<count; i++) {
            received[i] = false;
        }
        final Promise waitingArea = new Promise();
        long start = System.currentTimeMillis();

        new Thread(new Runnable() {
                public void run() {
                    for(int i=0; i<count; i++) {
                        Message msg = null;
                        try {
                            msg=new Message(null, localAddr, new Integer(i));
                        }
                        catch(IOException e) {
                            e.printStackTrace();
                        }
                        try {
                            byte[] buffer = Util.objectToByteBuffer(msg);
                            dos.writeUTF(groupName); 
                            dos.writeInt(0);
                            dos.writeInt(buffer.length);
                            dos.write(buffer, 0, buffer.length); 
                            dos.flush();
                        }
                        catch(Exception e) {
                            // this fails the test
                            waitingArea.setResult(e);
                        }
                    }
                }
            }, "Sending Thread").start();


        new Thread(new Runnable() {
                public void run() {
                    int cnt=0;
                    while(cnt<count) {
                        try {
                            int len = dis.readInt();
                            byte[] buffer = new byte[len];
                            dis.readFully(buffer, 0, len);
                            Message msg = 
                                (Message)Util.objectFromByteBuffer(buffer);
                            int index = ((Integer)msg.getObject()).intValue();
                            received[index] = true;
                            cnt++;
                        }
                        catch(Exception e) {
                            // this fails the test
                            waitingArea.setResult(e);
                        }
                    }
                    waitingArea.setResult(Boolean.TRUE);
                }
            }, "Receiving Thread").start();

        
        // wait here the stress threads to finish
        Object result = waitingArea.getResult((long)timeout*1000);
        long stop = System.currentTimeMillis();
        
        // close the routing connection
        dis.close();
        dos.close();
        s.close();

        int messok = 0;
        for(int i=0; i<count; i++) {
            if (received[i]) {
                messok++;
            }
        }
        
        if (result == null) {
            fail("Timeout while waiting for all messages to be received. "+
                 messok+" messages out of "+count+" received so far.");
        }
        if (result instanceof Exception) {
            throw (Exception)result;
        }

        // make sure all messages have been received
        boolean success = true;
        for(int i=0; i<count; i++) {
            if (!received[i]) {
                fail("At least message "+i+" NOT RECEIVED");
            }
        }
        System.out.println("STRESS TEST OK, "+count+" messages, "+
                           1000*count/(stop-start)+" messages/sec");
    }




    public static Test suite() {
	TestSuite s=new TestSuite(RouterTest.class);
	return s;
    }
    
    public static void main(String[] args) {
	junit.textui.TestRunner.run(suite());
	System.exit(0);
    }

}
