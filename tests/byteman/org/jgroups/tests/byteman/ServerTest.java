package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.BaseServer;
import org.jgroups.blocks.cs.NioServer;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.util.Bits;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Tests concurrent connection establishments in TcpServer
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.BYTEMAN,singleThreaded=true,dataProvider="configProvider")
public class ServerTest extends BMNGRunner {
    protected BaseServer               a, b;
    protected static final InetAddress loopback;
    protected MyReceiver               receiver_a, receiver_b;
    protected static final int         PORT_A, PORT_B;
    public    static Address           A=null, B=null; // need to be static for the byteman rule scripts to access them
    protected static final String      STRING_A="a.req", STRING_B="b.req";
    protected static final int         NUM_SENDERS=50;


    static {
        try {
            loopback=Util.getLoopback();
            PORT_A=ResourceManager.getNextTcpPort(loopback);
            PORT_B=ResourceManager.getNextTcpPort(loopback);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    @DataProvider
    protected Object[][] configProvider() {
        return new Object[][] {
          {create(false, PORT_A), create(false, PORT_B)},
          {create(true, PORT_A), create(true, PORT_B)}
        };
    }

    protected void setup(BaseServer one, BaseServer two) throws Exception {
        setup(one,two, true);
    }

    protected void setup(BaseServer one, BaseServer two, boolean use_peer_conns) throws Exception {
        a=one;
        a.usePeerConnections(use_peer_conns);
        b=two;
        b.usePeerConnections(use_peer_conns);
        A=a.localAddress();
        B=b.localAddress();
        assert A.compareTo(B) < 0;
        a.receiver(receiver_a=new MyReceiver("A"));
        a.start();
        b.receiver(receiver_b=new MyReceiver("B"));
        b.start();
    }

    @AfterMethod
    protected void destroy() {
        Util.close(a, b);
    }


    public void testStart(BaseServer a, BaseServer b) throws Exception {
        setup(a, b);
        assert !a.hasConnection(B) && !b.hasConnection(A);
        assert a.getNumConnections() == 0 && b.getNumConnections() == 0;
    }

    public void testSimpleSend(BaseServer a, BaseServer b) throws Exception {
        setup(a,b);
        send(STRING_A, a, B);
        check(receiver_b.getList(), STRING_A);
    }


    /**
     * Tests A connecting to B, and then B connecting to A; no concurrent connections
     */
    public void testSimpleConnection(BaseServer first, BaseServer second) throws Exception {
        setup(first,second);
        send("hello", a, B);
        waitForOpenConns(1, a, b);
        assert a.getNumOpenConnections() == 1 : "number of connections for conn_a: " + a.getNumOpenConnections();
        assert b.getNumOpenConnections() == 1 : "number of connections for conn_b: " + b.getNumOpenConnections();
        check(receiver_b.getList(),"hello");

        send("hello", b, A);
        waitForOpenConns(1, a, b);
        assert a.getNumOpenConnections() == 1 : "number of connections for conn_a: " + a.getNumOpenConnections();
        assert b.getNumOpenConnections() == 1 : "number of connections for conn_b: " + b.getNumOpenConnections();
        check(receiver_b.getList(), "hello");
        check(receiver_a.getList(), "hello");
    }


    /**
     * Tests the case where A and B connect to each other concurrently:
     * <ul>
     * <li>A.accept(B) || B.accept(A)</li>
     * <li>A.connect(B) || B.connect(A)</li>
     * </ul>
     */
    @BMScript(dir="scripts/ServerTest", value="testConcurrentConnect")
    // @Test(dataProvider="configProvider",invocationCount=5)
    public void testConcurrentConnect(BaseServer first, BaseServer second) throws Exception {
        setup(first, second);
        _testConcurrentConnect(1, 1, 0);
    }

    /**
     * Tests multiple threads sending a message to the same (unconnected) server; the first thread should establish
     * the connection to the server and the other threads should be blocked until the connection has been created.<br/>
     * JIRA: https://issues.jboss.org/browse/JGRP-2271
     */
    // @Test(invocationCount=50,dataProvider="configProvider")
    public void testConcurrentConnect2(BaseServer first, BaseServer second) throws Exception {
        setup(first, second, false);
        final CountDownLatch latch=new CountDownLatch(1);
        Sender2[] senders=new Sender2[NUM_SENDERS];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender2(latch, first, B, String.valueOf(i));
            senders[i].start();
        }
        latch.countDown();
        for(Thread sender: senders)
            sender.join();

        final List<String> list=receiver_b.getList();
        for(int i=0; i < 10; i++) {
            if(list.size() == NUM_SENDERS)
                break;
            Util.sleep(1000);
        }
        assert list.size() == NUM_SENDERS : String.format("list (%d elements): %s", list.size(), list);
        for(int i=0; i < list.size(); i++)
            assert list.contains(String.valueOf(i));
    }



    protected void _testConcurrentConnect(int expected_msgs_in_A, int expected_msgs_in_B, int alt_b) throws Exception {
        new Thread(new Sender(a,B, STRING_A), "sender-1").start();
        new Thread(new Sender(b,A, STRING_B), "sender-2").start();

        waitForOpenConns(1, a, b);

        assert a.getNumOpenConnections() == 1
          : "expected 1 connection but got " + a.getNumOpenConnections() + ": " + a.printConnections();

        assert b.getNumOpenConnections() == 1
          : "expected 1 connection but got " + b.getNumOpenConnections() + ": " + b.printConnections();

        List<String> list_a=receiver_a.getList();
        List<String> list_b=receiver_b.getList();

        for(int i=0; i < 10; i++) {
            if(list_a.size() == expected_msgs_in_A && (list_b.size() == expected_msgs_in_B || list_b.size() == alt_b))
                break;
            Util.sleep(500);
        }
        System.out.println("list A=" + list_a + " (expected=" + expected_msgs_in_A + ")" +
                             "\nlist B=" + list_b + "( expected=" + expected_msgs_in_B + " or " + alt_b + ")");
        assert list_a.size() == expected_msgs_in_A && (list_b.size() == expected_msgs_in_B || list_b.size() == alt_b)
          : "list A=" + list_a + "\nlist B=" + list_b;
    }



    protected static void check(List<String> list, String expected_str) {
        for(int i=0; i < 20; i++) {
            if(list.isEmpty())
                Util.sleep(500);
            else
                break;
        }
        assert !list.isEmpty() && list.get(0).equals(expected_str) : " list: " + list + ", expected " + expected_str;
    }


    protected static void waitForOpenConns(int expected, BaseServer... servers) {
        for(int i=0; i < 10; i++) {
            boolean all_ok=true;
            for(BaseServer server: servers) {
                if(server.getNumOpenConnections() != expected) {
                    all_ok=false;
                    break;
                }
            }
            if(all_ok)
                return;
            Util.sleep(500);
        }
    }


    protected static BaseServer create(boolean nio, int port) {
        try {
            return nio? new NioServer(loopback, port) : new TcpServer(loopback, port);
        }
        catch(Exception ex) {
            return null;
        }
    }

    protected static void send(String str, BaseServer server, Address dest) {
        byte[] request=str.getBytes();
        byte[] data=new byte[request.length + Global.INT_SIZE];
        Bits.writeInt(request.length, data, 0);
        System.arraycopy(request, 0, data, Global.INT_SIZE, request.length);
        try {
            server.send(dest, data, 0, data.length);
        }
        catch(Exception e) {
            System.err.println("Failed sending a request to " + dest + ": " + e);
        }
    }


    protected static class Sender implements Runnable {
        protected final BaseServer server;
        protected final Address    dest;
        protected final String     req_to_send;


        public Sender(BaseServer server, Address dest, String req_to_send) {
            this.server=server;
            this.dest=dest;
            this.req_to_send=req_to_send;
        }

        public void run() {
            send(req_to_send, server, dest);
        }
    }

    protected static class Sender2 extends Thread {
        protected final CountDownLatch latch;
        protected final BaseServer     server;
        protected final Address        dest;
        protected final String         payload;

        public Sender2(CountDownLatch latch, BaseServer server, Address dest, String payload) {
            this.latch=latch;
            this.server=server;
            this.dest=dest;
            this.payload=payload;
        }

        public void run() {
            try {
                latch.await();
                send(payload, server, dest);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected static class MyReceiver extends ReceiverAdapter {
        protected final String        name;
        protected final List<String>  reqs=new ArrayList<>();

        public MyReceiver(String name) {
            this.name=name;
        }

        public List<String> getList() {return reqs;}
        public void         clear()   {reqs.clear();}

        public void receive(Address sender, byte[] data, int offset, int length) {
            int len=Bits.readInt(data, offset);
            String str=new String(data, offset+Global.INT_SIZE, len);
            System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
            synchronized(reqs) {
                reqs.add(str);
            }
        }

        public void receive(Address sender, DataInput in) throws Exception {
            int len=in.readInt();
            byte[] data=new byte[len];
            in.readFully(data, 0, data.length);
            String str=new String(data, 0, data.length);
            System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
            synchronized(reqs) {
                reqs.add(str);
            }
        }
    }



}
