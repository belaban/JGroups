package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.cs.BaseServer;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.blocks.cs.NioServer;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

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


    static {
        try {
            loopback=Util.getLocalhost();
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
        a=one;
        a.usePeerConnections(true);
        b=two;
        b.usePeerConnections(true);
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
        byte[] data=STRING_A.getBytes();
        a.send(B, data, 0, data.length);
        check(receiver_b.getList(), STRING_A);
    }


    /**
     * Tests A connecting to B, and then B connecting to A; no concurrent connections
     */
    // @Test(dataProvider="configProvider",invocationCount=5)
    public void testSimpleConnection(BaseServer first, BaseServer second) throws Exception {
        setup(first,second);
        byte[] buf="hello".getBytes();
        a.send(B, buf, 0, buf.length);
        waitForOpenConns(1, a, b);
        assert a.getNumOpenConnections() == 1 : "number of connections for conn_a: " + a.getNumOpenConnections();
        assert b.getNumOpenConnections() == 1 : "number of connections for conn_b: " + b.getNumOpenConnections();
        check(receiver_b.getList(),"hello");

        b.send(A, buf, 0, buf.length);
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



    protected void check(List<String> list, String expected_str) {
        for(int i=0; i < 20; i++) {
            if(list.isEmpty())
                Util.sleep(500);
            else
                break;
        }
        assert !list.isEmpty() && list.get(0).equals(expected_str) : " list: " + list + ", expected " + expected_str;
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


    protected BaseServer create(boolean nio, int port) {
        try {
            return nio? new NioServer(loopback, port) : new TcpServer(loopback, port);
        }
        catch(Exception ex) {
            return null;
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
            byte[] request=req_to_send.getBytes();
            try {
                server.send(dest, request, 0, request.length);
            }
            catch(Exception e) {
                System.err.println("Failed sending a request to " + dest + ": " + e);
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
            String str=new String(data, offset, length);
            System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
            reqs.add(str);
        }
    }



}
