package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * @author Bela Ban
 * @since  5.3.2
 */
@Test(groups= Global.FUNCTIONAL,singleThreaded=true,dataProvider="configProvider")
public class ServerTests {
    protected BaseServer               a, b;
    protected static final InetAddress loopback;
    protected MyReceiver               receiver_a, receiver_b;
    protected static int               PORT_A, PORT_B, NUM_SENDERS=50;
    public    static PhysicalAddress   A=null, B=null; // need to be static for the byteman rule scripts to access them
    protected static final String      STRING_A="a.req";


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
        a=one.usePeerConnections(use_peer_conns);
        b=two.usePeerConnections(use_peer_conns);
        A=a.localAddress();
        B=b.localAddress();
        assert A.compareTo(B) < 0;
        a.receiver(receiver_a=new MyReceiver("A").verbose(false));
        a.start();
        b.receiver(receiver_b=new MyReceiver("B").verbose(false));
        b.start();
    }

    @AfterMethod
    protected void destroy() throws TimeoutException {
        Util.close(a,b);
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
     * Tests multiple threads sending a message to the same (unconnected) server; the first thread should establish
     * the connection to the server and the other threads should be blocked until the connection has been created.<br/>
     * JIRA: https://issues.redhat.com/browse/JGRP-2271
     */
    // @Test(invocationCount=50,dataProvider="configProvider")
    public void testConcurrentConnect(BaseServer first, BaseServer second) throws Exception {
        setup(first, second, false);
        final List<String> list=receiver_b.getList();
        final CountDownLatch latch=new CountDownLatch(1);
        Sender[] senders=new Sender[NUM_SENDERS];
        for(int i=0; i < senders.length; i++) {
            senders[i]=new Sender(latch, first, B, receiver_b.getList());
            senders[i].start();
        }
        latch.countDown();
        for(Sender sender: senders)
            sender.join();
        List<String> ids=Arrays.stream(senders).map(t -> String.valueOf(t.getId())).collect(Collectors.toList());
        Util.waitUntil(3000, 100, () -> list.size() == NUM_SENDERS,
                       () -> {
                           list.sort(String::compareTo);
                           return String.format("list (%d): %s", list.size(), list);
                       });
        list.sort(String::compareTo);
        ids.sort(String::compareTo);
        System.out.printf("list (%d elements): %s\n", list.size(), list);
        assert ids.equals(list) : String.format("expected:\n%s\nactual:\n%s\n", ids, list);
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
            return nio? new NioServer(loopback, port)
              : new TcpServer(loopback, port);
        }
        catch(Exception ex) {
            return null;
        }
    }

    protected static void sendOld(String str, BaseServer server, PhysicalAddress dest) throws Exception {
        byte[] request=str.getBytes();
        byte[] data=new byte[request.length + Global.INT_SIZE];
        Bits.writeInt(request.length, data, 0);
        System.arraycopy(request, 0, data, Global.INT_SIZE, request.length);
        server.send(dest, data, 0, data.length);
    }

    protected static void send(String str, BaseServer server, PhysicalAddress dest) throws Exception {
        byte[] request=str.getBytes();
        server.send(dest, request, 0, request.length);
    }


    protected static class Sender extends Thread {
        protected final CountDownLatch  latch;
        protected final BaseServer      server;
        protected final PhysicalAddress dest;
        protected final List<String>    receiver;

        public Sender(CountDownLatch latch, BaseServer server, PhysicalAddress dest, List<String> r) {
            this.latch=latch;
            this.server=server;
            this.dest=dest;
            this.receiver=r;
        }

        public void run() {
            try {
                latch.await();
                String payload=String.valueOf(Thread.currentThread().getId());
                send(payload, server, dest);
            }
            catch(Exception ex) {
                System.err.printf("[%d]: %s\n", getId(), ex);
            }
        }
    }

    protected static class MyReceiver extends ReceiverAdapter {
        protected final String        name;
        protected final List<String>  reqs=new ArrayList<>();
        protected boolean             verbose=true;

        public MyReceiver(String name) {
            this.name=name;
        }

        public List<String> getList()          {return reqs;}
        public void         clear()            {reqs.clear();}
        public int          size()             {synchronized(reqs) {return reqs.size();}}
        public MyReceiver   verbose(boolean v) {verbose=v; return this;}


        public void receive(PhysicalAddress sender, byte[] data, int offset, int length) {
            String str=new String(data, offset, length);
            if(verbose)
                System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
            synchronized(reqs) {
                reqs.add(str);
            }
        }

        public void receive(PhysicalAddress sender, DataInput in, int length) throws Exception {
            byte[] data=new byte[length];
            in.readFully(data, 0, data.length);
            String str=new String(data);
            if(verbose)
                System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
            synchronized(reqs) {
                reqs.add(str);
            }
        }
    }

}
