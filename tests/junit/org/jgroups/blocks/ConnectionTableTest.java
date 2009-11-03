package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.blocks.BasicConnectionTable.Connection;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;

import EDU.oswego.cs.dl.util.concurrent.CyclicBarrier;


/**
 * Tests ConnectionTable
 * @author Bela Ban
 * @version $Id: ConnectionTableTest.java,v 1.2.2.5 2009/11/03 03:49:01 rachmatowicz Exp $
 */
public class ConnectionTableTest extends TestCase {
    private BasicConnectionTable ct1, ct2;
    static String bind_addr_str=null;
    static InetAddress bind_addr=null;
    static byte[] data=new byte[]{'b', 'e', 'l', 'a'};
    Address addr1, addr2;
    int active_threads=0;
    // needed as OSs need not release ports immediately after a socket has closed
    int port_range = 3 ;
    
    final static int PORT1=7521, PORT2=8931;

    static {
        try {
        	bind_addr_str = System.getProperty("jgroups.bind_addr", "127.0.0.1") ;
            bind_addr=InetAddress.getByName(bind_addr_str);
        }
        catch(UnknownHostException e) {
            e.printStackTrace();
        }
    }


    public ConnectionTableTest(String testName) {
        super(testName);
    }


    protected void setUp() throws Exception {
        super.setUp();
        active_threads=Thread.activeCount();
        System.out.println("active threads before (" + active_threads + "):\n" + Util.activeThreads());
        addr1=new IpAddress(bind_addr, PORT1);
        addr2=new IpAddress(bind_addr, PORT2);
    }


    protected void tearDown() throws Exception {
        if(ct2 != null) {
            ct2.stop();
            ct2=null;
        }
        if(ct1 != null) {
            ct1.stop();
            ct1=null;
        }
        super.tearDown();
    }

    /**
     * A connects to B and B connects to A at the same time. This test makes sure we only have <em>one</em> connection,
     * not two, e.g. a spurious connection. Tests http://jira.jboss.com/jira/browse/JGRP-549
     */
    public void testConcurrentConnect() throws Exception {
        Sender sender1, sender2;
        CyclicBarrier barrier=new CyclicBarrier(3);

        ct1=new ConnectionTable(bind_addr, PORT1);
        ct1.start();
        ct2=new ConnectionTable(bind_addr, PORT2);
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

        barrier.attemptBarrier(10*1000);
        sender1.join();
        sender2.join();
       
        
        System.out.println("ct1: " + ct1 + "\nct2: " + ct2);
        num_conns=ct1.getNumConnections();
        assert num_conns == 1 : "num_conns is " + num_conns;
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
                barrier.attemptBarrier(10*1000);
                if(sleep_time > 0)
                    Util.sleep(sleep_time);
                conn_table.send(dest, data, 0, data.length);
            }
            catch(Exception e) {
            }
        }
    }


    public void testStopConnectionTable() throws Exception {
        ct1=new ConnectionTable(new DummyReceiver(), bind_addr, null, PORT1, PORT1+port_range, 60000, 120000);
        ct2=new ConnectionTable(new DummyReceiver(), bind_addr, null, PORT2, PORT2+port_range, 60000, 120000);
        // don't forget to start the connection tables and their acceptor thread
        ct1.start();
        ct2.start();
        _testStop(ct1, ct2);
    }

    public void testStopConnectionTableNIO() throws Exception {
        ct1=new ConnectionTableNIO(new DummyReceiver(), bind_addr, null, PORT1, PORT1+port_range, 60000, 120000, false);
        ct2=new ConnectionTableNIO(new DummyReceiver(), bind_addr, null, PORT2, PORT2+port_range, 60000, 120000, false);
        // don't forget to start the connection tables and their acceptor thread
        ct1.start();
        ct2.start();
        _testStop(ct1, ct2);
    }


    private void _testStop(BasicConnectionTable table1, BasicConnectionTable table2) throws Exception {
        table1.send(addr1, data, 0, data.length); // send to self
        assertEquals(0, table1.getNumConnections()); // sending to self should not create a connection
        table1.send(addr2, data, 0, data.length); // send to other

        table2.send(addr2, data, 0, data.length); // send to self
        table2.send(addr1, data, 0, data.length); // send to other
        
        System.out.println("table1:\n" + table1 + "\ntable2:\n" + table2);

        assertEquals(1, table1.getNumConnections());
        assertEquals(1, table2.getNumConnections());

        table2.stop();
        table1.stop();
        assertEquals(0, table1.getNumConnections());
        assertEquals(0, table2.getNumConnections());

        Util.sleep(1000);

        int current_active_threads=Thread.activeCount();
        System.out.println("active threads after (" + current_active_threads + "):\n" + Util.activeThreads());
        // System.out.println("thread:\n" + dumpThreads());
        assertEquals(active_threads, current_active_threads);
    }




    /* CAUTION: JDK 5 specific code


    private String dumpThreads() {
        StringBuffer sb=new StringBuffer();
        ThreadMXBean bean=ManagementFactory.getThreadMXBean();
        long[] ids=bean.getAllThreadIds();
        ThreadInfo[] threads=bean.getThreadInfo(ids, 20);
        for(int i=0; i < threads.length; i++) {
            ThreadInfo info=threads[i];
            if(info == null)
                continue;
            sb.append(info.getThreadName()).append(":\n");
            StackTraceElement[] stack_trace=info.getStackTrace();
            for(int j=0; j < stack_trace.length; j++) {
                StackTraceElement el=stack_trace[j];
                sb.append("at ").append(el.getClassName()).append(".").append(el.getMethodName());
                sb.append("(").append(el.getFileName()).append(":").append(el.getLineNumber()).append(")");
                sb.append("\n");
            }
            sb.append("\n\n");
        }
        return sb.toString();
    }
    */

    public static Test suite() {
        return new TestSuite(ConnectionTableTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(ConnectionTableTest.suite());
    }

    static class DummyReceiver implements ConnectionTable.Receiver {
        public void receive(Address sender, byte[] data, int offset, int length) {
        }
    }

    static class DummyReceiverNIO implements ConnectionTableNIO.Receiver {
        public void receive(Address sender, byte[] data, int offset, int length) {
        }
    }

}
