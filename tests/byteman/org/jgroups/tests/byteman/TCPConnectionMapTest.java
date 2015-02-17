package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.TCPConnectionMap;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests concurrent connection establishments in TCPConnectionMap
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.BYTEMAN,singleThreaded=true)
public class TCPConnectionMapTest extends BMNGRunner {
    protected TCPConnectionMap    conn_a, conn_b;
    protected InetAddress         loopback;
    protected MyReceiver          receiver_a, receiver_b;
    protected int                 PORT_A, PORT_B;
    public    static Address      A=null, B=null; // need to be static for the byteman rule scripts to access them
    protected static final String STRING_A="a.req", STRING_B="b.req";
    protected static final String RSP_A="a.rsp",    RSP_B="b.rsp";


    @BeforeMethod
    protected void setup() throws Exception {
        loopback=InetAddress.getByName("127.0.0.1");
        // Order the addresses: A should be greater than B; A.compareTo(B) > 1
        PORT_A=ResourceManager.getNextTcpPort(loopback);
        PORT_B=ResourceManager.getNextTcpPort(loopback);
        A=new IpAddress(loopback, PORT_A);
        B=new IpAddress(loopback, PORT_B);
        System.out.println("A=" + A + ", B=" + B);
        assert A.compareTo(B) < 0;
        conn_a=createConnectionMap(loopback, PORT_A);
        conn_b=createConnectionMap(loopback, PORT_B);
        receiver_a=new MyReceiver("A"); receiver_b=new MyReceiver("B");
    }

    @AfterMethod
    protected void destroy() {
        if(conn_a != null)
            conn_a.stop();
        if(conn_b != null)
            conn_b.stop();
    }



    /**
     * Tests A connecting to B, and then B connecting to A; no concurrent connections
     */
    public void testSimpleConnection() throws Exception {
        conn_a.setReceiver(receiver_a);
        conn_a.start();
        conn_b.setReceiver(receiver_b);
        conn_b.start();
        byte[] buf="hello".getBytes();
        conn_a.send(B, buf, 0, buf.length);
        waitForOpenConns(1, conn_a, conn_b);
        assert conn_a.getNumOpenConnections() == 1 : "number of connections for conn_a: " + conn_a.getNumOpenConnections();
        assert conn_b.getNumOpenConnections() == 1 : "number of connections for conn_b: " + conn_b.getNumOpenConnections();
        check(receiver_b.getList(),"hello");

        conn_b.send(A, buf, 0, buf.length);
        waitForOpenConns(1, conn_a, conn_b);
        assert conn_a.getNumOpenConnections() == 1 : "number of connections for conn_a: " + conn_a.getNumOpenConnections();
        assert conn_b.getNumOpenConnections() == 1 : "number of connections for conn_b: " + conn_b.getNumOpenConnections();
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
    @BMScript(dir="scripts/TCPConnectionMapTest", value="testConcurrentConnect")
    // @Test(invocationCount=100,threadPoolSize=0)
    public void testConcurrentConnect() throws Exception {
        _testConcurrentConnect(1, 1, 1);
    }


    /**
     * Tests the case where A and B connect to each other concurrently, but A gets B's connection *after* creating
     * its own connection to B. A will close its own connection and will therefore be able to send its message to B
     * (unless we implement some resending)
     */
    @BMScript(dir="scripts/TCPConnectionMapTest", value="testConcurrentConnect2")
    // @Test(invocationCount=100,threadPoolSize=0)
    public void testConcurrentConnect2() throws Exception {
        _testConcurrentConnect(1,
                               1, 0); // 1 or 0 messages
    }




    protected void _testConcurrentConnect(int expected_msgs_in_A, int expected_msgs_in_B, int alt_b) throws Exception {
        conn_a.setReceiver(receiver_a);
        conn_b.setReceiver(receiver_b);
        conn_a.start();
        conn_b.start();

        Thread sender_a=new Thread(new Sender(conn_a,B, STRING_A));
        sender_a.start();
        Thread sender_b=new Thread(new Sender(conn_b,A, STRING_B));
        sender_b.start();

        waitForOpenConns(1, conn_a, conn_b);

        assert conn_a.getNumOpenConnections() == 1
          : "expected 1 connection but got " + conn_a.getNumOpenConnections() + ": " + conn_a.printConnections();

        assert conn_b.getNumOpenConnections() == 1
          : "expected 1 connection but got " + conn_b.getNumOpenConnections() + ": " + conn_b.printConnections();

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


    protected TCPConnectionMap createConnectionMap(InetAddress bind_addr, int port) throws Exception {
        return new TCPConnectionMap("conn", new DefaultThreadFactory("ConnectionMapTest", true, true),
                                    new DefaultSocketFactory(), null, bind_addr, null, 0, port,port);
    }

    protected void check(List<String> list, String expected_str) {
        for(int i=0; i < 20; i++) {
            if(list.isEmpty())
                Util.sleep(500);
            else
                break;
        }
        assert !list.isEmpty() && list.get(0).equals(expected_str) : " list: " + list + ", expected " + expected_str;
    }

    protected void waitForOpenConns(int expected, TCPConnectionMap... maps) {
        for(int i=0; i < 10; i++) {
            boolean all_ok=true;
            for(TCPConnectionMap map: maps) {
                if(map.getNumOpenConnections() != expected) {
                    all_ok=false;
                    break;
                }
            }
            if(all_ok)
                return;
            Util.sleep(500);
        }
    }




    protected static class Sender implements Runnable {
        protected final TCPConnectionMap map;
        protected final Address          dest;
        protected final String           req_to_send;


        public Sender(TCPConnectionMap map, Address dest, String req_to_send) {
            this.map=map;
            this.dest=dest;
            this.req_to_send=req_to_send;
        }

        public void run() {
            byte[] request=req_to_send.getBytes();
            try {
                map.send(dest, request, 0, request.length);
            }
            catch(Exception e) {
                System.err.println("Failed sending a request to " + dest + ": " + e);
            }
        }
    }

    protected static class MyReceiver implements TCPConnectionMap.Receiver {
        protected final String        name;
        protected final List<String>  reqs=new ArrayList<>();

        public MyReceiver(String name) {
            this.name=name;
        }

        public List<String> getList() {return reqs;}
        public void         clear()   {reqs.clear();}

        public void receive(Address sender, byte[] data, int offset, int length) {
            String str=new String(data, offset, length);
            System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
            reqs.add(str);
        }
    }



}
