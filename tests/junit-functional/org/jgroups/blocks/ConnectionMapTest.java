package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.StackType;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * Tests ConnectionMap
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class ConnectionMapTest {
    private TCPConnectionMap ct1, ct2;
    static final InetAddress loopback_addr;

    static {
        try {
            StackType type=Util.getIpStackType();
            String tmp=type == StackType.IPv6? "::1" : "127.0.0.1";
            loopback_addr=InetAddress.getByName(tmp);
        }
        catch(UnknownHostException e) {
            throw new RuntimeException("failed initializing loopback_addr", e);
        }
    }

    static byte[] data={'b', 'e', 'l', 'a'};


    protected int PORT1, PORT2;
    protected Address addr1, addr2;


    @BeforeMethod
    protected void init() throws Exception {
        List<Short> ports=ResourceManager.getNextTcpPorts(loopback_addr, 2);
        PORT1=ports.get(0);
        PORT2=ports.get(1);
        addr1=new IpAddress(loopback_addr, PORT1);
        addr2=new IpAddress(loopback_addr, PORT2);
    }



    @AfterMethod
    protected void tearDown() throws Exception {
        if(ct2 != null) {
            ct2.stop();
            ct2=null;
        }
        if(ct1 != null) {
            ct1.stop();
            ct1=null;
        }
    }

    /**
     * A connects to B and B connects to A at the same time. This test makes sure we only have <em>one</em> connection,
     * not two, e.g. a spurious connection. Tests http://jira.jboss.com/jira/browse/JGRP-549.<p/>
     * Turned concurrent test into a simple sequential test. We're going to replace this code with NIO2 soon anyway...
     */
    public void testReuseOfConnection() throws Exception {
        TCPConnectionMap.Receiver dummy=new TCPConnectionMap.Receiver() {
            public void receive(Address sender, byte[] data, int offset, int length) {}
        };

        ct1=new TCPConnectionMap("ConnectionMapTest1",
                                 new DefaultThreadFactory("ConnectionMapTest", true),
                                 null, dummy, loopback_addr, null, 0, PORT1, PORT1);
        ct1.start();
        
        ct2=new TCPConnectionMap("ConnectionMapTest2",
                                 new DefaultThreadFactory("ConnectionMapTest", true),
                                 null, dummy, loopback_addr, null, 0, PORT2, PORT2);
        ct2.start();
        
        int num_conns;
        num_conns=ct1.getNumConnections();
        assert num_conns == 0;
        num_conns=ct2.getNumConnections();
        assert num_conns == 0;

        ct1.send(addr2, data, 0, data.length);
        ct2.send(addr1, data, 0, data.length);

        String msg="ct1: " + ct1 + "\nct2: " + ct2;
        System.out.println(msg);

        num_conns=ct1.getNumConnections();
        assert num_conns == 1 : "num_conns for ct1 is " + num_conns + ", " + msg;
        num_conns=ct2.getNumConnections();
        assert num_conns == 1 : "num_conns for ct2 is " + num_conns + ", " + msg;

        assert ct1.connectionEstablishedTo(addr2) : "valid connection to peer";
        assert ct2.connectionEstablishedTo(addr1) : "valid connection to peer";
    }




    public static void testBlockingQueue() {
        final BlockingQueue queue=new LinkedBlockingQueue();

        Thread taker=new Thread() {

            public void run() {
                try {
                    System.out.println("taking an element from the queue");
                    queue.take();
                    System.out.println("clear");
                }
                catch(InterruptedException e) {                	
                }
            }
        };
        taker.start();

        Util.sleep(500);

        queue.clear(); // does this release the taker thread ?
        Util.interruptAndWaitToDie(taker);
        assert !(taker.isAlive()) : "taker: " + taker;
    }


    public void testStopConnectionMapNoSendQueues() throws Exception {
        ct1=new TCPConnectionMap("ConnectionMapTest1",
                                 new DefaultThreadFactory("ConnectionMapTest", true),
                                 new DummyReceiver(), loopback_addr, null, 0, PORT1, PORT1, 60000, 120000);
        ct1.setUseSendQueues(false);
        ct1.start();
        ct2=new TCPConnectionMap("ConnectionMapTest2",
                                 new DefaultThreadFactory("ConnectionMapTest", true),
                                 new DummyReceiver(), loopback_addr, null, 0, PORT2, PORT2, 60000, 120000);
        ct2.setUseSendQueues(false);
        ct2.start();
        _testStop(ct1, ct2);
    }

    public void testStopConnectionMapWithSendQueues() throws Exception {
        ct1=new TCPConnectionMap("ConnectionMapTest1",
                                 new DefaultThreadFactory("ConnectionMapTest", true),
                                 new DummyReceiver(), loopback_addr, null, 0, PORT1, PORT1, 60000, 120000);
        ct1.start();
        ct2=new TCPConnectionMap("ConnectionMapTest2",
                                 new DefaultThreadFactory("ConnectionMapTest", true),
                                 new DummyReceiver(), loopback_addr, null, 0, PORT2, PORT2, 60000, 120000);
        ct2.start();
        _testStop(ct1, ct2);
    }


   /* public void testStopConnectionMapNIONoSendQueues() throws Exception {
        ct1=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, PORT1, PORT1, 60000, 120000, false);
        ct1.setUseSendQueues(false);       
        ct2=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, PORT2, PORT2, 60000, 120000, false);
        ct2.setUseSendQueues(false);
        ct1.start();
        ct2.start();
        _testStop(ct1, ct2);
    }


    public void testStopConnectionMapNIOWithSendQueues() throws Exception {
        ct1=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, PORT1, PORT1, 60000, 120000, false);
        ct2=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, PORT2, PORT2, 60000, 120000, false);
        ct1.start();
        ct2.start();
        _testStop(ct1, ct2);
    }*/


    private void _testStop(TCPConnectionMap table1, TCPConnectionMap table2) throws Exception {
        table1.send(addr1, data, 0, data.length); // send to self
        assert table1.getNumConnections() == 0;
        table1.send(addr2, data, 0, data.length); // send to other

        table2.send(addr2, data, 0, data.length); // send to self
        table2.send(addr1, data, 0, data.length); // send to other


        System.out.println("table1:\n" + table1 + "\ntable2:\n" + table2);

        int num_conns_table1=table1.getNumConnections(), num_conns_table2=table2.getNumConnections();
        assert num_conns_table1 == 1 : "table1 should have 1 connection, but has " + num_conns_table1 + ": " + table1;
        assert num_conns_table2 == 1 : "table2 should have 1 connection, but has " + num_conns_table2 + ": " + table2;

        table2.stop();
        table1.stop();
        assert table1.getNumConnections() == 0  : "table1 should have 0 connections: " + table1;
        assert table2.getNumConnections() == 0  : "table2 should have 0 connections: " + table2;
    }




    static class DummyReceiver implements TCPConnectionMap.Receiver {
        public void receive(Address sender, byte[] data, int offset, int length) {
            System.out.println("-- received " + length + " bytes from " + sender);
        }
    }

}
