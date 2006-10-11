// $Id: RouterStubTest.java,v 1.1 2006/10/11 08:01:12 belaban Exp $

package org.jgroups.tests.stack;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Message;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.GossipData;
import org.jgroups.util.Util;

import java.io.*;
import java.net.Socket;
import java.util.Random;
import java.util.Vector;

/**
 * Tests routing protocol primitives with the new GossipRouter. Since 2.2.1,
 * the GossipRouter is supposed to answer Gossip requests too.
 * <p/>
 * Note: Disable DEBUG logging before this test, otherwise the stress tests
 * may timeout.
 *
 * @author Ovidiu Feodorov <ovidiuf@users.sourceforge.net>
 * @author Bela Ban
 * @version $Revision: 1.1 $
 * @since 2.2.1
 */
public class RouterStubTest extends TestCase {
    DataInputStream dis=null;
    DataOutputStream dos=null;
    Socket sock=null;

    private static final Log log = LogFactory.getLog(RouterStubTest.class);

    private int routerPort=-1;
    private Random random=new Random();

    public RouterStubTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        dis=null; dos=null; sock=null;
        routerPort=Utilities.startGossipRouter();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        Utilities.stopGossipRouter();
        Util.close(dis);
        Util.close(dos);
        Util.close(sock);
    }

    /**
     * Sends a GossipRouter.GET request to a router with an empty routing table.
     */
    public void testEmptyGET() throws Exception {
        log.info("running testEmptyGET");
        sock=new Socket("localhost", routerPort);
        dis=new DataInputStream(sock.getInputStream());
        dos=new DataOutputStream(sock.getOutputStream());

        // read the IpAddress sent by GossipRouter
        IpAddress localAddr=(IpAddress)Util.readAddress(dis);
        assertEquals(localAddr.getIpAddress(), sock.getLocalAddress());
        assertEquals(localAddr.getPort(), sock.getLocalPort());

        // send GET request
        GossipData req=new GossipData(GossipRouter.GET, "nosuchgroup", null, null);
        req.writeTo(dos);

        // read the answer
        GossipData rsp=new GossipData();
        rsp.readFrom(dis);
    }


    /**
     * Sends a GossipRouter.CONNECT request followed by a GossipRouter.GET for the
     * group just registered.
     */
    public void test_CONNECT_GET() throws Exception {
        log.info("running test_REGISTER_GET");
        String groupName="TESTGROUP";

        sock=new Socket("localhost", routerPort);
        dis=new DataInputStream(sock.getInputStream());
        dos=new DataOutputStream(sock.getOutputStream());

        // read the IpAddress sent by GossipRouter
        IpAddress localAddr=(IpAddress)Util.readAddress(dis);
        assertEquals(localAddr.getIpAddress(), sock.getLocalAddress());
        assertEquals(localAddr.getPort(), sock.getLocalPort());

        // send CONNECT request
        GossipData req=new GossipData(GossipRouter.CONNECT, groupName, localAddr, null);
        req.writeTo(dos);
        Util.close(dos);
        Util.close(dis);
        Util.close(sock);

        // registration is complete, send a GET request
        sock=new Socket("localhost", routerPort);
        dis=new DataInputStream(sock.getInputStream());
        dos=new DataOutputStream(sock.getOutputStream());

        // read the IpAddress sent by GossipRouter
        localAddr=(IpAddress)Util.readAddress(dis);
        assertEquals(localAddr.getIpAddress(), sock.getLocalAddress());
        assertEquals(localAddr.getPort(), sock.getLocalPort());

        // send GET request
        req=new GossipData(GossipRouter.GET, groupName, null, null);
        req.writeTo(dos);

        // read the answer
        GossipData rsp=new GossipData();
        rsp.readFrom(dis);
        Vector groupList=rsp.getMembers();
        assertEquals(1, groupList.size());
        assertEquals(localAddr, groupList.remove(0));
    }

    /**
     * Sends a GossipRouter.CONNECT request followed by a series of simple routing requests (to all
     * members of the group, to itself, to an inexistent member).
     */

//    public void test_CONNECT_Route_To_Self() throws Exception {
//        log.info("running test_REGISTER_Route_To_Self");
//
//        String groupName="TESTGROUP";
//        Message msg;
//
//        sock=new Socket("localhost", routerPort);
//        dis=new DataInputStream(sock.getInputStream());
//        dos=new DataOutputStream(sock.getOutputStream());
//
//        // read the IpAddress sent by GossipRouter
//        IpAddress localAddr=(IpAddress)Util.readAddress(dis);
//        assertEquals(localAddr.getIpAddress(), sock.getLocalAddress());
//        assertEquals(localAddr.getPort(), sock.getLocalPort());
//
//        // send REGISTER request
//        dos.writeInt(GossipRouter.REGISTER);
//        dos.writeUTF(groupName);
//
//        // send the Address back to the router
//        buffer=Util.objectToByteBuffer(localAddr);
//        dos.writeInt(buffer.length);
//        dos.write(buffer, 0, buffer.length);
//        dos.flush();
//
//        // registration is complete
//
//        String payload="THIS IS A MESSAGE PAYLOAD " + random.nextLong();
//
//        // send a simple routing request to all members (null dest address)
//        msg=new Message(null, localAddr, payload);
//        buffer=Util.objectToByteBuffer(msg);
//        dos.writeUTF(groupName);
//        dos.write(0); // a 0 byte means a null address
//        dos.writeInt(buffer.length);
//        dos.write(buffer, 0, buffer.length);
//
//        // due to Bela's optimizations, the router won't loopback local messages, the RouterStub
//        // is expected to loop them back, so the following section is useless. The router will
//        // just discard the message.
//
//        // send a simple routing request to itself
//        msg=new Message(localAddr, localAddr, payload);
//        buffer=Util.objectToByteBuffer(msg);
//        dos.writeUTF(groupName);
//        destAddrBuffer=Util.objectToByteBuffer(localAddr);
//        dos.writeInt(destAddrBuffer.length);
//        dos.write(destAddrBuffer, 0, destAddrBuffer.length);
//        dos.writeInt(buffer.length);
//        dos.write(buffer, 0, buffer.length);
//
//        // due to Bela's optimizations, the router won't loopback local messages, the RouterStub
//        // is expected to loop them back, so the following section is useless. The router will
//        // just discard the message.
//
//        // send a simple routing request to an inexistent member, the message
//        // should be discarded by router
//        Address inexistentAddress=
//                new IpAddress("localhost", Utilities.getFreePort());
//
//        msg=new Message(inexistentAddress, localAddr, payload);
//        buffer=Util.objectToByteBuffer(msg);
//        dos.writeUTF(groupName);
//        destAddrBuffer=Util.objectToByteBuffer(inexistentAddress);
//        dos.writeInt(destAddrBuffer.length);
//        dos.write(destAddrBuffer, 0, destAddrBuffer.length);
//        dos.writeInt(buffer.length);
//        dos.write(buffer, 0, buffer.length);
//
//        // the message should be discarded by router
//
//        // close the routing connection
//        dis.close();
//        dos.close();
//        s.close();
//    }
//
//
//    public void test_REGISTER_Route_To_All() throws Exception {
//
//        log.info("running test_REGISTER_Route_To_All");
//
//        int len;
//        byte[] buffer;
//        String groupName="TESTGROUP";
//        Message msg, msgCopy;
//
//        // Register the first member
//
//        Socket sOne = new Socket("localhost", routerPort);
//        DataInputStream disOne = new DataInputStream(sOne.getInputStream());
//        DataOutputStream dosOne = new DataOutputStream(sOne.getOutputStream());
//
//        // read the IpAddress sent by GossipRouter
//        len=disOne.readInt();
//        buffer=new byte[len];
//        disOne.readFully(buffer, 0, len);
//        IpAddress localAddrOne=(IpAddress)Util.objectFromByteBuffer(buffer);
//        assertEquals(localAddrOne.getIpAddress(), sOne.getLocalAddress());
//        assertEquals(localAddrOne.getPort(), sOne.getLocalPort());
//
//        // send REGISTER request
//        dosOne.writeInt(GossipRouter.REGISTER);
//        dosOne.writeUTF(groupName);
//
//        // send the Address back to the router
//        buffer=Util.objectToByteBuffer(localAddrOne);
//        dosOne.writeInt(buffer.length);
//        dosOne.write(buffer, 0, buffer.length);
//        dosOne.flush();
//
//        // registration of the first member is complete
//
//        // Register the second member
//
//        Socket sTwo = new Socket("localhost", routerPort);
//        DataInputStream disTwo = new DataInputStream(sTwo.getInputStream());
//        DataOutputStream dosTwo = new DataOutputStream(sTwo.getOutputStream());
//
//        // read the IpAddress sent by GossipRouter
//        len=disTwo.readInt();
//        buffer=new byte[len];
//        disTwo.readFully(buffer, 0, len);
//        IpAddress localAddrTwo=(IpAddress)Util.objectFromByteBuffer(buffer);
//        assertEquals(localAddrTwo.getIpAddress(), sTwo.getLocalAddress());
//        assertEquals(localAddrTwo.getPort(), sTwo.getLocalPort());
//
//        // send REGISTER request
//        dosTwo.writeInt(GossipRouter.REGISTER);
//        dosTwo.writeUTF(groupName);
//
//        // send the Address back to the router
//        buffer=Util.objectToByteBuffer(localAddrTwo);
//        dosTwo.writeInt(buffer.length);
//        dosTwo.write(buffer, 0, buffer.length);
//        dosTwo.flush();
//
//        // registration of the second member is complete
//
//        // make sure both clients registered
//        Thread.sleep(1000);
//
//        String payload="THIS IS A MESSAGE PAYLOAD " + random.nextLong();
//
//        // the first member sends a simple routing request to all members (null dest address)
//        msg=new Message(null, localAddrOne, payload);
//
//        writeMessage(groupName, msg, dosOne);
//
//        // only the second member should receive the routing request, the router won't send a
//        // message to the originator
//
//        // the second member reads the message
//        msgCopy=readMessage(disTwo);
//        assertEquals(msg.getSrc(), msgCopy.getSrc());
//        assertNull(msgCopy.getDest());
//        assertEquals(msg.getObject(), msgCopy.getObject());
//
//
//        // close the routing connection
//        disOne.close();
//        dosOne.close();
//        sOne.close();
//        disTwo.close();
//        dosTwo.close();
//        sTwo.close();
//    }
//
//
//    public void test_REGISTER_Route_To_Other() throws Exception {
//        log.info("running test_REGISTER_Route_To_Other");
//
//        int len;
//        byte[] buffer;
//        String groupName="TESTGROUP";
//        Message msg, msgCopy;
//
//        // Register the first member
//        Socket sOne = new Socket("localhost", routerPort);
//        DataInputStream disOne = new DataInputStream(sOne.getInputStream());
//        DataOutputStream dosOne = new DataOutputStream(sOne.getOutputStream());
//
//        // read the IpAddress sent by GossipRouter
//        len=disOne.readInt();
//        buffer=new byte[len];
//        disOne.readFully(buffer, 0, len);
//        IpAddress localAddrOne=(IpAddress)Util.objectFromByteBuffer(buffer);
//        assertEquals(localAddrOne.getIpAddress(), sOne.getLocalAddress());
//        assertEquals(localAddrOne.getPort(), sOne.getLocalPort());
//
//        // send REGISTER request
//        dosOne.writeInt(GossipRouter.REGISTER);
//        dosOne.writeUTF(groupName);
//
//        // send the Address back to the router
//        buffer=Util.objectToByteBuffer(localAddrOne);
//        dosOne.writeInt(buffer.length);
//        dosOne.write(buffer, 0, buffer.length);
//        dosOne.flush();
//
//        // registration of the first member is complete
//
//        // Register the second member
//
//        Socket sTwo = new Socket("localhost", routerPort);
//        DataInputStream disTwo = new DataInputStream(sTwo.getInputStream());
//        DataOutputStream dosTwo = new DataOutputStream(sTwo.getOutputStream());
//
//        // read the IpAddress sent by GossipRouter
//        len=disTwo.readInt();
//        buffer=new byte[len];
//        disTwo.readFully(buffer, 0, len);
//        IpAddress localAddrTwo=(IpAddress)Util.objectFromByteBuffer(buffer);
//        assertEquals(localAddrTwo.getIpAddress(), sTwo.getLocalAddress());
//        assertEquals(localAddrTwo.getPort(), sTwo.getLocalPort());
//
//        // send REGISTER request
//        dosTwo.writeInt(GossipRouter.REGISTER);
//        dosTwo.writeUTF(groupName);
//
//        // send the Address back to the router
//        buffer=Util.objectToByteBuffer(localAddrTwo);
//        dosTwo.writeInt(buffer.length);
//        dosTwo.write(buffer, 0, buffer.length);
//        dosTwo.flush();
//
//        // registration of the second member is complete
//
//        // make sure both clients registered
//        Thread.sleep(1000);
//
//        String payload="THIS IS A MESSAGE PAYLOAD " + random.nextLong();
//
//        // first member send a simple routing request to the second member
//        msg=new Message(localAddrTwo, localAddrOne, payload);
//        writeMessage(groupName, msg, dosOne);
//
//        // the second member reads the message
//        msgCopy=readMessage(disTwo);
//        assertEquals(msg.getSrc(), msgCopy.getSrc());
//        assertEquals(msg.getDest(), msgCopy.getDest());
//        assertEquals(msg.getObject(), msgCopy.getObject());
//
//        // close the routing connection
//        disOne.close();
//        dosOne.close();
//        sOne.close();
//        disTwo.close();
//        dosTwo.close();
//        sTwo.close();
//    }
//
//
//
//
//
//    /**
//     * Sends a GossipRouter.REGISTER request followed by a series of stress routing
//     * requests to all members of the group.
//     */
//    public void test_REGISTER_RouteStressAll() throws Exception {
//        log.info("running test_REGISTER_RouteStressAll, this may take a while .... ");
//
//        int len;
//        byte[] buffer;
//        final String groupName="TESTGROUP";
//
//        // Register the first member
//        Socket sOne = new Socket("localhost", routerPort);
//        DataInputStream disOne = new DataInputStream(sOne.getInputStream());
//        final DataOutputStream dosOne = new DataOutputStream(sOne.getOutputStream());
//
//        // read the IpAddress sent by GossipRouter
//        len=disOne.readInt();
//        buffer=new byte[len];
//        disOne.readFully(buffer, 0, len);
//        final IpAddress localAddrOne=(IpAddress)Util.objectFromByteBuffer(buffer);
//        assertEquals(localAddrOne.getIpAddress(), sOne.getLocalAddress());
//        assertEquals(localAddrOne.getPort(), sOne.getLocalPort());
//
//        // send REGISTER request
//        dosOne.writeInt(GossipRouter.REGISTER);
//        dosOne.writeUTF(groupName);
//
//        // send the Address back to the router
//        buffer=Util.objectToByteBuffer(localAddrOne);
//        dosOne.writeInt(buffer.length);
//        dosOne.write(buffer, 0, buffer.length);
//        dosOne.flush();
//
//        // registration of the first member is complete
//
//        // Register the second member
//
//        Socket sTwo = new Socket("localhost", routerPort);
//        final DataInputStream disTwo = new DataInputStream(sTwo.getInputStream());
//        DataOutputStream dosTwo = new DataOutputStream(sTwo.getOutputStream());
//
//        // read the IpAddress sent by GossipRouter
//        len=disTwo.readInt();
//        buffer=new byte[len];
//        disTwo.readFully(buffer, 0, len);
//        IpAddress localAddrTwo=(IpAddress)Util.objectFromByteBuffer(buffer);
//        assertEquals(localAddrTwo.getIpAddress(), sTwo.getLocalAddress());
//        assertEquals(localAddrTwo.getPort(), sTwo.getLocalPort());
//
//        // send REGISTER request
//        dosTwo.writeInt(GossipRouter.REGISTER);
//        dosTwo.writeUTF(groupName);
//
//        // send the Address back to the router
//        buffer=Util.objectToByteBuffer(localAddrTwo);
//        dosTwo.writeInt(buffer.length);
//        dosTwo.write(buffer, 0, buffer.length);
//        dosTwo.flush();
//
//        // registration of the second member is complete
//
//        // make sure both clients registered
//        Thread.sleep(1000);
//
//        // send a series of stress routing requests to all members
//        final int count=20000; // total number of messages to be sent
//        int timeout=50; // nr of secs to wait for all messages to arrrive
//
//        final boolean[] received=new boolean[count];
//        for(int i=0; i < count; i++) {
//            received[i]=false;
//        }
//        final Promise waitingArea=new Promise();
//        long start=System.currentTimeMillis();
//
//        new Thread(new Runnable() {
//            public void run() {
//                for(int i=0; i < count; i++) {
//                    Message msg=new Message(null, localAddrOne, new Integer(i));
//                    try {
//                        writeMessage(groupName, msg, dosOne);
//                        if(i % 2000 == 0)
//                            System.out.println("--sent " + i);
//                    }
//                    catch(Exception e) {
//                        // this fails the test
//                        waitingArea.setResult(e);
//                    }
//                }
//            }
//        }, "Sending Thread").start();
//
//
//        new Thread(new Runnable() {
//            public void run() {
//                int cnt=0;
//                while(cnt < count) {
//                    try {
//                        Message msg=readMessage(disTwo);
//                        int index=((Integer)msg.getObject()).intValue();
//                        received[index]=true;
//                        cnt++;
//                        if(cnt % 2000 == 0)
//                            System.out.println("-- received " + cnt);
//                    }
//                    catch(Exception e) {
//                        // this fails the test
//                        waitingArea.setResult(e);
//                    }
//                }
//                waitingArea.setResult(Boolean.TRUE);
//            }
//        }, "Receiving Thread").start();
//
//
//        // wait here the stress threads to finish
//        Object result=waitingArea.getResult((long)timeout * 1000);
//        long stop=System.currentTimeMillis();
//
//        // close the routing connection
//        disOne.close();
//        dosOne.close();
//        sOne.close();
//        disTwo.close();
//        dosTwo.close();
//        sTwo.close();
//
//
//        int messok=0;
//        for(int i=0; i < count; i++) {
//            if(received[i]) {
//                messok++;
//            }
//        }
//
//        if(result == null) {
//            fail("Timeout while waiting for all messages to be received. " +
//                    messok + " messages out of " + count + " received so far.");
//        }
//        if(result instanceof Exception) {
//            throw (Exception)result;
//        }
//
//        // make sure all messages have been received
//        for(int i=0; i < count; i++) {
//            if(!received[i]) {
//                fail("At least message " + i + " NOT RECEIVED");
//            }
//        }
//        System.out.println("STRESS TEST OK, " + count + " messages, " +
//                1000 * count / (stop - start) + " messages/sec");
//    }


    private void writeMessage(String group_name, Message msg, DataOutputStream out) throws IOException {
        ByteArrayOutputStream bos=new ByteArrayOutputStream(100);
        DataOutputStream tmp=new DataOutputStream(bos);
        msg.writeTo(tmp);
        tmp.flush();
        byte[] buffer=bos.toByteArray();
        tmp.close();

        // 1. group name first
        out.writeUTF(group_name);

        // 2. destination address
        Util.writeAddress(msg.getDest(), out);

        // 3. length of message
        out.writeInt(buffer.length);

        // 4. message
        out.write(buffer, 0, buffer.length);
        out.flush();
    }

    private Message readMessage(DataInputStream in) throws Exception {
        Address dest=Util.readAddress(in);
        int len=in.readInt();
        byte[] buffer=new byte[len];
        in.readFully(buffer, 0, len);
        Message msgCopy=new Message(false);
        ByteArrayInputStream tmp_in=new ByteArrayInputStream(buffer);
        DataInputStream inp=new DataInputStream(tmp_in);
        msgCopy.readFrom(inp);
        msgCopy.setDest(dest);
        tmp_in.close();
        return msgCopy;
    }


    public static Test suite() {
        TestSuite s=new TestSuite(RouterStubTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
        System.exit(0);
    }



}
