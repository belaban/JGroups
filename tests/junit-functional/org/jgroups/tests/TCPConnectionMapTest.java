package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.blocks.TCPConnectionMap;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.DefaultSocketFactory;
import org.jgroups.util.DefaultThreadFactory;
import org.jgroups.util.ResourceManager;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Tests concurrent connection establishments in TCPConnectionMap
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class TCPConnectionMapTest {
    protected TCPConnectionMap          conn_a, conn_b;
    protected static final InetAddress  loopback;
    protected Member                    member_a, member_b;
    protected static final int          PORT_A, PORT_B;
    protected static final Address      a, b;
    protected static final String       STRING_A="a.req", STRING_B="b.req";
    protected static final String       RSP_A="a.rsp",    RSP_B="b.rsp";

    static {
        try {
            loopback=InetAddress.getByName("127.0.0.1");
            PORT_A=ResourceManager.getNextTcpPort(loopback);
            PORT_B=ResourceManager.getNextTcpPort(loopback);
            a=new IpAddress(loopback, PORT_A);
            b=new IpAddress(loopback, PORT_B);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test(invocationCount=5,threadPoolSize=0)
    public void testConcurrentConnections() throws Exception {
        System.out.println("PORT_A=" + PORT_A + ", PORT_B=" + PORT_B);

        conn_a=createConnectionMap(loopback, PORT_A, null);
        member_a=new Member("A", conn_a, b, STRING_A, RSP_B, RSP_A);
        conn_a.setReceiver(member_a);
        conn_a.start();

        conn_b=createConnectionMap(loopback, PORT_B, null);
        member_b=new Member("B", conn_b, a, STRING_B, RSP_A, RSP_B);
        conn_b.setReceiver(member_b);
        conn_b.start();

        Thread sender_a=new Thread(member_a);
        sender_a.start();
        Thread sender_b=new Thread(member_b);
        sender_b.start();

        check(member_a.getList(), STRING_B);
        check(member_b.getList(), STRING_A);

        conn_b.stop();
        conn_a.stop();
    }

    protected TCPConnectionMap createConnectionMap(InetAddress bind_addr, int port,
                                                   TCPConnectionMap.Receiver receiver) throws Exception {
        return new TCPConnectionMap("conn", new DefaultThreadFactory("ConnectionMapTest", true, true),
                                    new DefaultSocketFactory(), receiver, bind_addr, null, 0, port,port);
    }

    protected void check(List<String> list, String expected_str) {
        for(int i=0; i < 20; i++) {
            if(list.isEmpty())
                Util.sleep(500);
            else
                break;
        }
        assert !list.isEmpty() && list.get(0).equals(expected_str) : " list: " + list;
    }

    /** Sends a message to a destination and waits for the response (mimicking UNICAST{2})*/
    protected static class Member implements Runnable, TCPConnectionMap.Receiver {
        protected final String           name;
        protected final TCPConnectionMap map;
        protected final Address          dest;
        protected final String           req_to_send;
        protected final String           rsp_to_send;
        protected final String           rsp_to_receive;
        protected volatile boolean       rsp_received;
        protected final List<String>     reqs=new ArrayList<String>();


        public Member(String name, TCPConnectionMap map, Address dest, String req_to_send, String rsp_to_send, String rsp_to_receive) {
            this.name=name;
            this.map=map;
            this.dest=dest;
            this.req_to_send=req_to_send;
            this.rsp_to_send=rsp_to_send;
            this.rsp_to_receive=rsp_to_receive;
        }

        public List<String> getList() {return reqs;}
        public void         clear()   {reqs.clear();}

        public void receive(Address sender, byte[] data, int offset, int length) {
            String str=new String(data, offset, length);

            // response received
            if(str.equals(rsp_to_receive)) {
                System.out.println("[" + name + "] received response \"" + str + "\" from " + sender);
                rsp_received=true;
                return;
            }

            // received request
            if(!reqs.contains(str)) {
                System.out.println("[" + name + "] received request \"" + str + "\" from " + sender);
                reqs.add(str);
            }

            // send response
            byte[] response=rsp_to_send.getBytes();
            try {
                map.send(sender, response, 0, response.length);
            }
            catch(Exception e) {
                System.err.println("Failed sending a response to " + sender + ": " + e);
            }
        }


        public void run() {
            while(!rsp_received) {
                byte[] buf=req_to_send.getBytes();
                try {
                    map.send(dest, buf, 0, buf.length);
                    Util.sleepRandom(300, 1000);
                }
                catch(Exception e) {
                    System.err.println("Failed sending a message to " + dest + ": " + e);
                    Util.sleepRandom(300, 1000);
                }
            }
        }
    }


}
