package org.jgroups.tests.byteman;

import org.jboss.byteman.contrib.bmunit.BMNGRunner;
import org.jboss.byteman.contrib.bmunit.BMScript;
import org.jgroups.Global;
import org.jgroups.PhysicalAddress;
import org.jgroups.blocks.cs.BaseServer;
import org.jgroups.blocks.cs.NioServer;
import org.jgroups.blocks.cs.ReceiverAdapter;
import org.jgroups.blocks.cs.TcpServer;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.DataInput;
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
    public    static PhysicalAddress   A=null, B=null; // need to be static for the byteman rule scripts to access them
    protected static final String      STRING_A="a.req", STRING_B="b.req";


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
        setup(first, second, true);
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
            if(list_a.size() == 1 && (list_b.size() == 1 || list_b.isEmpty()))
                break;
            Util.sleep(500);
        }
        System.out.println("list A=" + list_a + " (expected=1)" +
                             "\nlist B=" + list_b + "( expected=1 or 0)");
        assert list_a.size() == 1 && (list_b.size() == 1 || list_b.isEmpty())
          : "list A=" + list_a + "\nlist B=" + list_b;
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

    protected static void send(String str, BaseServer server, PhysicalAddress dest) {
        byte[] request=str.getBytes();
        try {
            server.send(dest, request, 0, request.length);
        }
        catch(Exception e) {
            System.err.println("Failed sending a request to " + dest + ": " + e);
        }
    }


    protected static class Sender implements Runnable {
        protected final BaseServer      server;
        protected final PhysicalAddress dest;
        protected final String          req_to_send;


        public Sender(BaseServer server, PhysicalAddress dest, String req_to_send) {
            this.server=server;
            this.dest=dest;
            this.req_to_send=req_to_send;
        }

        public void run() {
            send(req_to_send, server, dest);
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

        public void receive(PhysicalAddress sender, byte[] data, int offset, int length) {
            String str=new String(data, offset, length);
            System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
            synchronized(reqs) {
                reqs.add(str);
            }
        }

        public void receive(PhysicalAddress sender, DataInput in, int length) throws Exception {
            byte[] data=new byte[length];
            in.readFully(data, 0, data.length);
            String str=new String(data);
            System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
            synchronized(reqs) {
                reqs.add(str);
            }
        }
    }


}
