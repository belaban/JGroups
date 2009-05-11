package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;


/**
 * Tests ConnectionTable
 * @author Bela Ban
 * @version $Id: ConnectionTableTest.java,v 1.2.2.2 2009/05/11 17:43:15 rachmatowicz Exp $
 */
public class ConnectionTableTest extends TestCase {
    private BasicConnectionTable ct1, ct2;
    static String bind_addr_str=null;
    static InetAddress bind_addr=null;
    static byte[] data=new byte[]{'b', 'e', 'l', 'a'};
    Address addr1, addr2;
    int active_threads=0;
    
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



    public void testStopConnectionTable() throws Exception {
        ct1=new ConnectionTable(new DummyReceiver(), bind_addr, null, PORT1, PORT1, 60000, 120000);
        ct2=new ConnectionTable(new DummyReceiver(), bind_addr, null, PORT2, PORT2, 60000, 120000);
        _testStop(ct1, ct2);
    }

    public void testStopConnectionTableNIO() throws Exception {
        ct1=new ConnectionTableNIO(new DummyReceiver(), bind_addr, null, PORT1, PORT1, 60000, 120000, false);
        ct2=new ConnectionTableNIO(new DummyReceiver(), bind_addr, null, PORT2, PORT2, 60000, 120000, false);
        ct1.start();
        ct2.start();
        _testStop(ct1, ct2);
    }


    private void _testStop(BasicConnectionTable table1, BasicConnectionTable table2) throws Exception {
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

        // Util.sleep(1000);

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
