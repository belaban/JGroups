
package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.TUNNEL;
import org.jgroups.stack.GossipRouter;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;


/**
 * Ensures that a disconnected channel reconnects correctly, for different
 * stack configurations.
 *
 * @author Ovidiu Feodorov <ovidiu@feodorov.com>
 * @author Bela Ban belaban@yahoo.com
 * @version $Id: DisconnectTest.java,v 1.12.2.2 2008/10/30 13:59:24 belaban Exp $
 **/
public class DisconnectTest extends TestCase {

    private JChannel channel;

    public DisconnectTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if(channel != null) {
            channel.close();
            channel=null;
        }
    }



    private static JChannel createChannel() throws ChannelException {
        return new JChannel("tunnel.xml");
    }

    /**
     * Tests if the channel has a null local address after disconnect (using
     * TUNNEL).
     *
     * TO_DO: uncomment or delete after clarifying the semantics of
     * getLocalAddress() on a disconnected channel.
     *
     **/
    public void testNullLocalAddress_TUNNEL() throws Exception {
        GossipRouter router=null;
        try {
            router=new GossipRouter();
            router.start();
            channel = createChannel();
            channel.connect("testgroup");
            assertNotNull(channel.getLocalAddress());
            channel.disconnect();
            assertNull(channel.getLocalAddress());
        }
        finally {
            if(router != null)
                router.stop();
        }
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
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));
        coordinator.connect("testgroup");

        channel=new JChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");

        channel.send(new Message(null, null, "payload"));

        Message msg=(Message)msgPromise.getResult(20000);
        assertNotNull(msg);
        assertEquals("payload", msg.getObject());

        coordinator.close();
    }


    /**
      * Tests connect-disconnect-connect sequence for a group with one member
      * (using TUNNEL).
      **/
     public void testDisconnectConnectOne_TUNNEL() throws Exception {
        GossipRouter router=null;
        try {
            router=new GossipRouter();
            router.start();
            channel=createChannel();
            channel.connect("testgroup1");
            channel.disconnect();
            channel.connect("testgroup2");
            View view=channel.getView();
            assertEquals(1, view.size());
            assertTrue(view.containsMember(channel.getLocalAddress()));
        }
        finally {
            if(router != null)
                router.stop();
        }
     }


    public void testConnectTwo_TUNNEL() throws Exception {
        GossipRouter router=null;
         try {
             router=new GossipRouter();
             router.start();
             JChannel coordinator=createChannel();
             setProps(coordinator);
             coordinator.connect("testConnectTwo_TUNNEL");
             channel=createChannel();
             setProps(channel);
             channel.connect("testConnectTwo_TUNNEL");

             View view=channel.getView();
             for(int i=0; i < 10; i++) {
                 if(view.size() >= 2)
                     break;
                 System.out.print(".");
                 Util.sleep(1000);
                 view=channel.getView();
             }

             System.out.println("view = " + view);
             assertEquals(2, view.size());
             assertTrue(view.containsMember(channel.getLocalAddress()));
             assertTrue(view.containsMember(coordinator.getLocalAddress()));
             channel.close();
             coordinator.close();
         }
         finally {
             if(router != null)
                 router.stop();
         }
     }


     /**
      * Tests connect-disconnect-connect sequence for a group with two members
      * (using TUNNEL).
      **/
     public void testDisconnectConnectTwo_TUNNEL() throws Exception {
         GossipRouter router=null;
         try {
             router=new GossipRouter();
             router.start();
             JChannel coordinator=createChannel();
             setProps(coordinator);
             coordinator.connect("testgroup");
             channel=createChannel();
             setProps(channel);
             channel.connect("testgroup1");
             channel.disconnect();
             channel.connect("testgroup");

             View view=channel.getView();
             for(int i=0; i < 10; i++) {
                 if(view.size() >= 2)
                     break;
                 System.out.print(".");
                 Util.sleep(1000);
                 view=channel.getView();
             }

             assertEquals(2, view.size());
             assertTrue(view.containsMember(channel.getLocalAddress()));
             assertTrue(view.containsMember(coordinator.getLocalAddress()));
             channel.close();
             coordinator.close();
         }
         finally {
             if(router != null)
                 router.stop();
         }
     }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
      * members, using TUNNEL. Test case introduced before fixing pbcast.NAKACK
      * bug, which used to leave pbcast.NAKACK in a broken state after
      * DISCONNECT. Because of this problem, the channel couldn't be used to
      * multicast messages.
      **/
     public void testDisconnectConnectSendTwo_TUNNEL() throws Exception {
        GossipRouter router=null;
        try {
            router=new GossipRouter();
            router.start();
            final Promise msgPromise=new Promise();
            JChannel coordinator=createChannel();
            setProps(coordinator);
            coordinator.setReceiver(new PromisedMessageListener(msgPromise));
            coordinator.connect("testgroup");

            channel=createChannel();
            setProps(channel);
            channel.connect("testgroup1");
            channel.disconnect();
            channel.connect("testgroup");

            channel.send(new Message(null, null, "payload"));

            Message msg=(Message)msgPromise.getResult(20000);
            assertNotNull(msg);
            assertEquals("payload", msg.getObject());
            channel.close();
            coordinator.close();
        }
        finally {
            if(router != null)
                router.stop();
        }
    }


    private static void setProps(JChannel channel) {
        ProtocolStack stack=channel.getProtocolStack();
        MERGE2 merge=(MERGE2)stack.findProtocol(MERGE2.class);
        if(merge != null) {
            merge.setMinInterval(1000);
            merge.setMaxInterval(3000);
        }

        TUNNEL tunnel=(TUNNEL)stack.getTransport();
        if(tunnel != null) {
            tunnel.setReconnectInterval(2000);
        }

        PING ping=(PING)stack.findProtocol(PING.class);
        if(ping != null) {
            ping.setGossipRefresh(1000);
        }
    }


    public static Test suite() {
        return new TestSuite(DisconnectTest.class);
    }



    private static class PromisedMessageListener extends ReceiverAdapter {
        private final Promise promise;

        public PromisedMessageListener(Promise promise) {
            this.promise=promise;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }
    }

}
