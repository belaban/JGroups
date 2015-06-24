
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.BaseServer;
import org.jgroups.blocks.TcpServer;
import org.jgroups.nio.NioServer;
import org.jgroups.nio.ReceiverAdapter;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.InetAddress;


/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="configProvider")
public class ServerUnitTest {
    static final int     port1, port2, port3, port4;
    protected BaseServer s1, s2;


    static {
        try {
            InetAddress localhost=Util.getLocalhost();
            port1=ResourceManager.getNextTcpPort(localhost);
            port2=ResourceManager.getNextTcpPort(localhost);
            port3=ResourceManager.getNextTcpPort(localhost);
            port4=ResourceManager.getNextTcpPort(localhost);
        }
        catch(Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @DataProvider
    protected Object[][] configProvider() {
        return new Object[][] {
          {create(false, port1), create(false, port2)},
          {create(true, port3), create(true, port4)}
        };
    }


    protected void setup(BaseServer a, BaseServer b) throws Exception {
        s1=a; s2=b;
        s1.start();
        s2.start();
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(s1, s2);
    }

    public void testSetup(BaseServer a, BaseServer b) throws Exception {
        setup(a, b);
        Assert.assertNotSame(s1.localAddress(), s2.localAddress());
    }

    public void testSendToNullReceiver(BaseServer a, BaseServer b) throws Exception {
        setup(a,b);
        byte[]  data=new byte[0];
        try {
            s1.send(null, data, 0, data.length);
            assert false : "sending data to a null destination should have thrown an exception";
        }
        catch(IllegalArgumentException ex) {
            System.out.println("got exception as expected: " + ex);
        }
    }

    public void testSendEmptyData(BaseServer a, BaseServer b) throws Exception {
        setup(a,b);
        byte[]  data=new byte[0];
        Address myself=s1.localAddress();
        s1.receiver(new ReceiverAdapter<Address>() {
            public void receive(Address sender, byte[] data, int offset, int length) {
            }
        });
        s1.send(myself, data, 0, data.length);
    }

    public void testSendNullData(BaseServer a, BaseServer b) throws Exception {
        setup(a, b);
        Address myself=s1.localAddress();
        s1.send(myself, null, 0, 0);
    }


    public void testSendToSelf(BaseServer a, BaseServer b) throws Exception {
        setup(a,b);
        long       NUM=1000, total_time;
        Address    myself=s1.localAddress();
        MyReceiver r=new MyReceiver(s1, NUM, false);
        byte[]     data="hello world".getBytes();

        s1.receiver(r);

        for(int i=0; i < NUM; i++)
            s1.send(myself, data, 0, data.length);

        log("sent " + NUM + " msgs");
        r.waitForCompletion();
        total_time=r.stop_time - r.start_time;
        log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
            ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived()  + " ms/msg)");

        Assert.assertEquals(r.getNumExpected(), r.getNumReceived());
    }


    public void testSendToOther(BaseServer a, BaseServer b) throws Exception {
        setup(a,b);
        long       NUM=1000, total_time;
        Address    other=s2.localAddress();
        MyReceiver r=new MyReceiver(s2, NUM, false);
        byte[]     data="hello world".getBytes();

        s2.receiver(r);
        for(int i=0; i < NUM; i++)
            s1.send(other, data, 0, data.length);

        log("sent " + NUM + " msgs");
        r.waitForCompletion();
        total_time=r.stop_time - r.start_time;
        log("number expected=" + r.getNumExpected() + ", number received=" + r.getNumReceived() +
              ", total time=" + total_time + " (" + (double)total_time / r.getNumReceived() + " ms/msg)");

        Assert.assertEquals(r.getNumExpected(), r.getNumReceived());
    }


    public void testSendToOtherGetResponse(BaseServer a, BaseServer b) throws Exception {
        setup(a,b);
        long       NUM=1000, total_time;
        Address    other=s2.localAddress();
        MyReceiver r1=new MyReceiver(s1, NUM, false);
        MyReceiver r2=new MyReceiver(s2, NUM, true); // send response
        byte[]     data="hello world".getBytes();

        s1.receiver(r1);
        s2.receiver(r2);

        for(int i=0; i < NUM; i++)
            s1.send(other, data, 0, data.length);
        log("sent " + NUM + " msgs");
        r1.waitForCompletion();
        total_time=r1.stop_time - r1.start_time;
        log("number expected=" + r1.getNumExpected() + ", number received=" + r1.getNumReceived() +
            ", total time=" + total_time + " (" + (double)total_time / r1.getNumReceived()  + " ms/msg)");

        Assert.assertEquals(r1.getNumExpected(), r1.getNumReceived());
    }


    /**
     * A connects to B and B connects to A at the same time. This test makes sure we only have <em>one</em> connection,
     * not two, e.g. a spurious connection. Tests http://jira.jboss.com/jira/browse/JGRP-549.<p/>
     * Turned concurrent test into a simple sequential test. We're going to replace this code with NIO2 soon anyway...
     */
    public void testReuseOfConnection(BaseServer a, BaseServer b) throws Exception {
        setup(a, b);

        int num_conns;
        num_conns=s1.getNumConnections();
        assert num_conns == 0;
        num_conns=s2.getNumConnections();
        assert num_conns == 0;

        Address addr1=s1.localAddress(), addr2=s2.localAddress();
        byte[] data="hello world".getBytes();

        s1.send(addr2, data, 0, data.length);
        s2.send(addr1, data, 0, data.length);

        String msg="ct1: " + s1 + "\nct2: " + s2;
        System.out.println(msg);

        waitForOpenConns(1, s1, s2);
        num_conns=s1.getNumOpenConnections();
        assert num_conns == 1 : "num_conns for ct1 is " + num_conns + ", " + msg;
        num_conns=s2.getNumOpenConnections();
        assert num_conns == 1 : "num_conns for ct2 is " + num_conns + ", " + msg;

        // done in a loop because connect() might be non-blocking (NioServer)
        connectionEstablished(s1, addr2);
        connectionEstablished(s2, addr1);
    }


    public void testConnectionCountOnStop(BaseServer a, BaseServer b) throws Exception {
        setup(a,b);
        Address addr1=s1.localAddress(), addr2=s2.localAddress();
        byte[] data="hello world".getBytes();
        s1.send(addr1, data, 0, data.length); // send to self
        assert s1.getNumConnections() == 0;
        s1.send(addr2, data, 0, data.length); // send to other

        s2.send(addr2, data, 0, data.length); // send to self
        s2.send(addr1, data, 0, data.length); // send to other


        System.out.println("s1:\n" + s1 + "\ns2:\n" + s2);

        int num_conns_table1=s1.getNumConnections(), num_conns_table2=s2.getNumConnections();
        assert num_conns_table1 == 1 : "s1 should have 1 connection, but has " + num_conns_table1 + ": " + s1;
        assert num_conns_table2 == 1 : "s2 should have 1 connection, but has " + num_conns_table2 + ": " + s2;

        Util.close(s2,s1);
        waitForOpenConns(0, s1,s2);
        assert s1.getNumOpenConnections() == 0  : "s1 should have 0 connections: " + s1.printConnections();
        assert s2.getNumOpenConnections() == 0  : "s2 should have 0 connections: " + s2.printConnections();
    }



    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread() + "]: " + msg);
    }

    protected BaseServer create(boolean nio, int port) {
        try {
            return nio? new NioServer(null, port) : new TcpServer(null, port).useSendQueues(false);
        }
        catch(Exception ex) {
            return null;
        }
    }

    protected void waitForOpenConns(int expected, BaseServer... servers) {
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


    protected void connectionEstablished(BaseServer server, Address dest) {
        for(int i=0; i < 10; i++) {
            if(server.connectionEstablishedTo(dest))
                break;
            Util.sleep(500);
        }
        assert server.connectionEstablishedTo(dest);
    }


    protected static class MyReceiver extends ReceiverAdapter<Address> {
        long             num_expected=0, num_received=0, start_time=0, stop_time=0;
        volatile boolean done=false;
        boolean          send_response=false;
        long             modulo;
        BaseServer       ct;

        MyReceiver(BaseServer ct, long num_expected, boolean send_response) {
            this.ct=ct;
            this.num_expected=num_expected;
            this.send_response=send_response;
            start_time=System.currentTimeMillis();
            modulo=num_expected / 10;
        }


        public long getNumReceived() {
            return num_received;
        }

        public long getNumExpected() {
            return num_expected;
        }


        public void receive(Address sender, byte[] data, int offset, int length) {
            num_received++;
            if(num_received % modulo == 0)
                log("received msg# " + num_received);
            if(send_response) {
                if(ct != null) {
                    try {
                        byte[] rsp=new byte[0];
                        ct.send(sender, rsp, 0, rsp.length);
                    }
                    catch(Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            if(num_received >= num_expected) {
                synchronized(this) {
                    if(!done) {
                        done=true;
                        stop_time=System.currentTimeMillis();
                        notifyAll();
                    }
                }
            }
        }


        public synchronized void waitForCompletion() {
            while(!done) {
                try {
                    wait();
                }
                catch(InterruptedException e) {
                }
            }
        }


    }

}
