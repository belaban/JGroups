// $Id: DisconnectTest.java,v 1.3 2004/03/01 03:27:50 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.MessageListener;
import org.jgroups.View;
import org.jgroups.blocks.PullPushAdapter;
import org.jgroups.log.Trace;
import org.jgroups.stack.GossipData;
import org.jgroups.stack.GossipServer;
import org.jgroups.stack.Router;
import org.jgroups.util.Promise;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;




/**
 * Ensures that a disconnected channel reconnects correctly, for different
 * stack configurations.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @version $Revision: 1.3 $
 **/
public class DisconnectTest extends TestCase {

    private JChannel channel;

    public DisconnectTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
        Trace.setTrace(true);
        Trace.setDefaultOutput(Trace.WARN, System.err);
    }

    public void tearDown() throws Exception {

        super.tearDown();

        // TO_DO: no elegant way to stop the Router/GossipServer threads and
        //        clean-up resources. Use the Router/GossipServer
        //        administrative interface.

        if(channel != null) {
            channel.close();
            channel=null;
        }
    }


    private String getTUNNELProps(int routerPort, int gossipPort) {
        return
                "TUNNEL(router_host=localhost;router_port=" + routerPort + "):" +
                "PING(gossip_host=localhost;gossip_port=" + gossipPort + "):" +
                "pbcast.FD:" +
                "VERIFY_SUSPECT(timeout=1500;down_thread=false;up_thread=false):" +
                "pbcast.NAKACK(gc_lag=100;retransmit_timeout=3000;" +
                "down_thread=true;up_thread=true):" +
                "pbcast.STABLE(desired_avg_gossip=20000;down_thread=false;" +
                "up_thread=false):" +
                "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;shun=false;" +
                "print_local_addr=false;down_thread=true;up_thread=true)";
    }



    /**
     * Tests if the channel has a null local address after disconnect (using
     * TUNNEL).
     *
     * TO_DO: uncomment or delete after clarifying the semantics of
     * getLocalAddress() on a disconnected channel.
     *
     **/
//     public void testNullLocalAddress_TUNNEL() throws Exception {

// 	String props = getTUNNELProps(startRouter(),startGossipServer());

//   	channel = new JChannel(props);
// 	channel.connect("testgroup");
// 	assertTrue(channel.getLocalAddress()!=null);
// 	channel.disconnect();
// 	assertNull(channel.getLocalAddress());
//     }



    /**
     * Tests connect-disconnect-connect sequence for a group with one member
     * (using TUNNEL).
     **/
    public void testDisconnectConnectOne_TUNNEL() throws Exception {

        String props=getTUNNELProps(startRouter(), startGossipServer());

        channel=new JChannel(props);
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup2");
        View view=channel.getView();
        assertEquals(1, view.size());
        assertTrue(view.containsMember(channel.getLocalAddress()));
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     * (using TUNNEL).
     **/
    public void testDisconnectConnectTwo_TUNNEL() throws Exception {

        String props=getTUNNELProps(startRouter(), startGossipServer());

        JChannel coordinator=new JChannel(props);
        coordinator.connect("testgroup");

        channel=new JChannel(props);
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");
        View view=channel.getView();
        assertEquals(2, view.size());
        assertTrue(view.containsMember(channel.getLocalAddress()));
        assertTrue(view.containsMember(coordinator.getLocalAddress()));

        coordinator.close();
    }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
     * members, using TUNNEL. Test case introduced before fixing pbcast.NAKACK
     * bug, which used to leave pbcast.NAKACK in a broken state after
     * DISCONNECT. Because of this problem, the channel couldn't be used to
     * multicast messages.
     **/
    public void testDisconnectConnectSendTwo_TUNNEL() throws Exception {

        String props=getTUNNELProps(startRouter(), startGossipServer());

        final Promise msgPromise=new Promise();
        JChannel coordinator=new JChannel(props);
        coordinator.connect("testgroup");
        PullPushAdapter ppa=
                new PullPushAdapter(coordinator,
                                    new PromisedMessageListener(msgPromise));
        ppa.start();

        channel=new JChannel(props);
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");

        channel.send(new Message(null, null, "payload"));

        Message msg=(Message)msgPromise.getResult(20000);
        assertTrue(msg != null);
        assertEquals("payload", msg.getObject());

        ppa.stop();
        coordinator.close();
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with one member
     * (using default configuration).
     **/
    public void testDisconnectConnectOne_Default() throws Exception {

        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup2");
        View view=channel.getView();
        assertEquals(1, view.size());
        assertTrue(view.containsMember(channel.getLocalAddress()));
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     * (using default configuration).
     **/
    public void testDisconnectConnectTwo_Default() throws Exception {

        JChannel coordinator=new JChannel();
        coordinator.connect("testgroup");

        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");
        View view=channel.getView();
        assertEquals(2, view.size());
        assertTrue(view.containsMember(channel.getLocalAddress()));
        assertTrue(view.containsMember(coordinator.getLocalAddress()));

        coordinator.close();
    }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
     * members, using the default stack configuration. Assumes that default
     * configuration includes pbcast.NAKACK. Test case introduced before fixing
     * pbcast.NAKACK bug, which used to leave pbcast.NAKACK in a broken state
     * after DISCONNECT. Because of this problem, the channel couldn't be used
     * to multicast messages.
     **/
    public void testDisconnectConnectSendTwo_Default() throws Exception {

        final Promise msgPromise=new Promise();
        JChannel coordinator=new JChannel();
        coordinator.connect("testgroup");
        PullPushAdapter ppa=
                new PullPushAdapter(coordinator,
                                    new PromisedMessageListener(msgPromise));
        ppa.start();

        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");

        channel.send(new Message(null, null, "payload"));

        Message msg=(Message)msgPromise.getResult(20000);
        assertTrue(msg != null);
        assertEquals("payload", msg.getObject());

        ppa.stop();
        coordinator.close();
    }


    public static Test suite() {
        TestSuite s=new TestSuite(DisconnectTest.class);
        return s;
    }

    public static void main(String[] args) {
        String[] testCaseName={DisconnectTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


    //
    // HELPERS
    //

    /**
     * Starts the router on a separate thread and makes sure it answers the
     * requests. Required by TUNNEL.
     **/
    private int startRouter() throws Exception {

        final int routerPort=getFreePort();
        Thread routerThread=new Thread(new Runnable() {
            public void run() {
                try {
                    new Router(routerPort).start();
                }
                catch(Exception e) {
                    System.err.println("Failed to start the router " +
                                       "on port " + routerPort);
                    e.printStackTrace();
                }
            }
        });
        routerThread.start();

        // verify the router - try to connect for 10 secs
        long startms=System.currentTimeMillis();
        long crtms=startms;
        Exception lastConnectException=null;
        while(crtms - startms < 10000) {
            Socket s=null;
            try {
                s=new Socket("localhost", routerPort);
            }
            catch(Exception e) {
                lastConnectException=e;
                Thread.sleep(1000);
                crtms=System.currentTimeMillis(); 
                continue;
            }
            lastConnectException=null;
            DataInputStream dis=new DataInputStream(s.getInputStream());
            DataOutputStream dos=new DataOutputStream(s.getOutputStream());

            // read the IpAddress
            int len=dis.readInt();
            byte[] buffer=new byte[len];
            dis.read(buffer, 0, len);

            // write a GET
            dos.writeInt(Router.GET);
            dos.writeUTF("nogroup_setup");
            dis.readInt();

            s.close();
            break;
        }
        if(lastConnectException != null) {
            lastConnectException.printStackTrace();
            fail("Cannot connect to the router");
        }
        System.out.println("router ok");
        return routerPort;
    }


    /**
     * Starts the gossip server on a separate thread and makes sure it answers
     * the requests. Required by TUNNEL.
     **/
    private int startGossipServer() throws Exception {

        final int gossipPort=getFreePort();
        Thread gossipThread=new Thread(new Runnable() {
            public void run() {
                try {
                    new GossipServer(gossipPort).run();
                }
                catch(Exception e) {
                    System.err.println("Failed to start the gossip " +
                                       "server on port " + gossipPort);
                    e.printStackTrace();
                }
            }
        });
        gossipThread.start();

        // verify the gossip server - try to connect for 10 secs
        long startms=System.currentTimeMillis();
        long crtms=startms;
        Exception lastConnectException=null;
        while(crtms - startms < 10000) {
            Socket s=null;
            try {
                s=new Socket("localhost", gossipPort);
            }
            catch(Exception e) {
                lastConnectException=e;
                Thread.sleep(1000);
                crtms=System.currentTimeMillis();
                continue;
            }
            lastConnectException=null;
            // send a null GossipData which will be silently discared
            ObjectOutputStream oos=
                    new ObjectOutputStream(s.getOutputStream());
            GossipData gd=null;
            oos.writeObject(gd);
            oos.close();
            s.close();
            break;
        }
        if(lastConnectException != null) {
            lastConnectException.printStackTrace();
            fail("Cannot connect to the gossip server");
        }
        System.out.println("gossip server ok");
        return gossipPort;
    }

    private int getFreePort() throws Exception {
        ServerSocket ss=new ServerSocket(0);
        int port=ss.getLocalPort();
        ss.close();
        return port;
    }


    private class PromisedMessageListener implements MessageListener {

        private Promise promise;

        public PromisedMessageListener(Promise promise) {
            this.promise=promise;
        }

        public byte[] getState() {
            return null;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }

        public void setState(byte[] state) {
        }
    }

}
