package org.jgroups.blocks;

import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.JChannel;
import org.jgroups.Channel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.RspList;
import org.jgroups.util.Rsp;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Vector;
import java.util.Iterator;
import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * Tests ConnectionTable
 * @author Bela Ban
 * @version $Id: ConnectionTableTest.java,v 1.1 2006/09/14 08:34:27 belaban Exp $
 */
public class ConnectionTableTest extends TestCase {
    private BasicConnectionTable ct1, ct2;
    static InetAddress loopback_addr=null;
    static byte[] data=new byte[]{'b', 'e', 'l', 'a'};
    Address addr1, addr2;

    static {
        try {
            loopback_addr=InetAddress.getByName("127.0.0.1");
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
        addr1=new IpAddress(loopback_addr, 7500);
        addr2=new IpAddress(loopback_addr, 8000);
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



    public void testStopConnectionTable() throws Exception {
        ct1=new ConnectionTable(new DummyReceiver(), loopback_addr, null, 7500, 10, 10, 300);
        ct2=new ConnectionTable(new DummyReceiver(), loopback_addr, null, 8000, 10, 10, 300);
        _testStop(ct1, ct2);
    }

    public void testStopConnectionTableNIO() throws Exception {
        ct1=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, 7500, 10, 10, 300);
        ct2=new ConnectionTableNIO(new DummyReceiver(), loopback_addr, null, 8000, 10, 10, 300);
        _testStop(ct1, ct2);
    }


    private void _testStop(BasicConnectionTable table1, BasicConnectionTable table2) throws Exception {
        int active_threads=Thread.activeCount();
        System.out.println("active threads before (" + active_threads + "):\n" + Util.activeThreads());

        table1.send(addr1, data, 0, data.length); // send to self
        table1.send(addr2, data, 0, data.length); // send to other

        table2.send(addr2, data, 0, data.length); // send to self
        table2.send(addr1, data, 0, data.length); // send to other

        assertEquals(2, table1.getNumConnections());
        assertEquals(2, table2.getNumConnections());

        table2.stop();
        table1.stop();
        assertEquals(0, table1.getNumConnections());
        assertEquals(0, table2.getNumConnections());

        int current_active_threads=Thread.activeCount();
        System.out.println("active threads after (" + current_active_threads + "):\n" + Util.activeThreads());
        assertEquals(active_threads, current_active_threads);
    }


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

}
