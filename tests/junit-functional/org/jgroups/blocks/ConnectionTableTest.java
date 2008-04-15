package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.BasicConnectionTable.Connection;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Tests ConnectionTable
 * @author Bela Ban
 * @version $Id: ConnectionTableTest.java,v 1.12 2008/04/15 12:37:22 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class ConnectionTableTest {
    private BasicConnectionTable ct1, ct2;
    static final InetAddress loopback_addr;

    static {
        try {
            loopback_addr=InetAddress.getByName("127.0.0.1");
        }
        catch(UnknownHostException e) {
            throw new RuntimeException("failed initializing loopback_addr", e);
        }
    }

    static byte[] data=new byte[]{'b', 'e', 'l', 'a'};
    final static int PORT1=7521, PORT2=8931;
    static final Address addr1=new IpAddress(loopback_addr, PORT1), addr2=new IpAddress(loopback_addr, PORT2);




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
     * not two, e.g. a spurious connection. Tests http://jira.jboss.com/jira/browse/JGRP-549
     */
    public void testConcurrentConnect() throws Exception {
        Sender sender1, sender2;
        CyclicBarrier barrier=new CyclicBarrier(3);

        ct1=new ConnectionTable(loopback_addr, PORT1);
        ct1.start();
        ct2=new ConnectionTable(loopback_addr, PORT2);
        ct2.start();
        BasicConnectionTable.Receiver dummy=new BasicConnectionTable.Receiver() {
            public void receive(Address sender, byte[] data, int offset, int length) {}
        };
        ct1.setReceiver(dummy);
        ct2.setReceiver(dummy);

        sender1=new Sender((ConnectionTable)ct1, barrier, addr2, 0);
        sender2=new Sender((ConnectionTable)ct2, barrier, addr1, 0);

        sender1.start(); sender2.start();
        Util.sleep(100);

        int num_conns;
        System.out.println("ct1: " + ct1 + "ct2: " + ct2);
        num_conns=ct1.getNumConnections();
        assert num_conns == 0;
        num_conns=ct2.getNumConnections();
        assert num_conns == 0;

        barrier.await(10000, TimeUnit.MILLISECONDS);
        sender1.join();
        sender2.join();
       
        
        System.out.println("ct1: " + ct1 + "\nct2: " + ct2);
        num_conns=ct1.getNumConnections();
        assert num_conns == 1;
        num_conns=ct2.getNumConnections();
        assert num_conns == 1;
        
        Util.sleep(500);
        
        System.out.println("ct1: " + ct1 + "\nct2: " + ct2);
        num_conns=ct1.getNumConnections();
        assert num_conns == 1;
        num_conns=ct2.getNumConnections();
        assert num_conns == 1;
        
        Connection connection = ct1.getConnection(addr2);
        assert !(connection.isSocketClosed()) : "valid connection to peer";
        connection = ct2.getConnection(addr1);
        assert !(connection.isSocketClosed()) : "valid connection to peer";
    }


    private static class Sender extends Thread {
        final ConnectionTable conn_table;
        final CyclicBarrier   barrier;
        final Address         dest;
        final long            sleep_time;

        public Sender(ConnectionTable conn_table, CyclicBarrier barrier, Address dest, long sleep_time) {
            this.conn_table=conn_table;
            this.barrier=barrier;
            this.dest=dest;
            this.sleep_time=sleep_time;
        }

        public void run() {
            try {
                barrier.await(10000, TimeUnit.MILLISECONDS);
                if(sleep_time > 0)
                    Util.sleep(sleep_time);
                conn_table.send(dest, data, 0, data.length);
            }
            catch(Exception e) {
            }
        }
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


    public void testStopConnectionTableNoSendQueues() throws Exception {
        ct1=new ConnectionTable(new DummyReceiver(), loopback_addr, null, PORT1, PORT1, 60000, 120000);
        ct1.setUseSendQueues(false);
        ct1.start();
        ct2=new ConnectionTable(new DummyReceiver(), loopback_addr, null, PORT2, PORT2, 60000, 120000);
        ct2.setUseSendQueues(false);
        ct2.start();
        _testStop(ct1, ct2);
    }

    public void testStopConnectionTableWithSendQueues() throws Exception {
        ct1=new ConnectionTable(new DummyReceiver(), loopback_addr, null, PORT1, PORT1, 60000, 120000);
        ct1.start();
        ct2=new ConnectionTable(new DummyReceiver(), loopback_addr, null, PORT2, PORT2, 60000, 120000);
        ct2.start();
        _testStop(ct1, ct2);
    }


    public void testStopConnectionTableNIONoSendQueues() throws Exception {
        ct1=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, PORT1, PORT1, 60000, 120000, false);
        ct1.setUseSendQueues(false);       
        ct2=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, PORT2, PORT2, 60000, 120000, false);
        ct2.setUseSendQueues(false);
        ct1.start();
        ct2.start();
        _testStop(ct1, ct2);
    }


    public void testStopConnectionTableNIOWithSendQueues() throws Exception {
        ct1=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, PORT1, PORT1, 60000, 120000, false);
        ct2=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, PORT2, PORT2, 60000, 120000, false);
        ct1.start();
        ct2.start();
        _testStop(ct1, ct2);
    }


    private static void _testStop(BasicConnectionTable table1, BasicConnectionTable table2) throws Exception {
        table1.send(addr1, data, 0, data.length); // send to self
        assert table1.getNumConnections() == 0;
        table1.send(addr2, data, 0, data.length); // send to other

        table2.send(addr2, data, 0, data.length); // send to self
        table2.send(addr1, data, 0, data.length); // send to other


        System.out.println("table1:\n" + table1 + "\ntable2:\n" + table2);

        assert table1.getNumConnections() == 1;
        assert table2.getNumConnections() == 1;

        table2.stop();
        table1.stop();
        assert table1.getNumConnections() == 0;
        assert table2.getNumConnections() == 0;
    }




    static class DummyReceiver implements ConnectionTable.Receiver {
        public void receive(Address sender, byte[] data, int offset, int length) {
            System.out.println("-- received " + length + " bytes from " + sender);
        }
    }

    static class DummyReceiverNIO implements ConnectionTableNIO.Receiver {
        public void receive(Address sender, byte[] data, int offset, int length) {
            System.out.println("-- received " + length + " bytes from " + sender);
        }
    }

}
