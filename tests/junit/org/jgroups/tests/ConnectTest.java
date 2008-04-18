
package org.jgroups.tests;


import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Runs through multiple channel connect and disconnects, without closing the channel.
 * @version $Id: ConnectTest.java,v 1.14 2008/04/18 10:18:37 belaban Exp $
 */
@Test(sequential=true)
public class ConnectTest extends ChannelTestBase {
    Channel channel;
    static final int TIMES=10;


    @AfterMethod
    public void tearDown() throws Exception {
        if(channel != null) {
            channel.close();
            channel = null;
        }
    }

    void doIt(int times) {
        for(int i=0; i < times; i++) {
            System.out.println("\nAttempt #" + (i + 1));
            System.out.print("Connecting to channel: ");
            try {
                channel.connect("ConnectTest");
                System.out.println("-- connected: " + channel.getView() + " --");
            }
            catch(Exception e) {
                System.out.println("-- connection failed --");
                System.err.println(e);
            }
            System.out.print("Disconnecting from channel: ");
            channel.disconnect();
            System.out.println("-- disconnected --");
        }
    }


    @Test
    public void testConnectAndDisconnect() throws Exception {
        System.out.print("Creating channel: ");
        channel=createChannel();
        System.out.println("-- created --");
        doIt(TIMES);
        System.out.print("Closing channel: ");
        channel.close();
        System.out.println("-- closed --");
    }

    @Test
    public void testDisconnectConnectOne() throws Exception {
        channel=createChannel();
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup2");
        View view=channel.getView();
        assert view.size() == 1;
        assert view.containsMember(channel.getLocalAddress());
        channel.close();
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     **/
    @Test
    public void testDisconnectConnectTwo() throws Exception {
        View     view;
        Channel coordinator=createChannel("A");
        coordinator.connect("testgroup");
        view=coordinator.getView();
        System.out.println("-- view for coordinator: " + view);

        channel=createChannel("A");
        channel.connect("testgroup1");
        view=channel.getView();
        System.out.println("-- view for channel: " + view);

        channel.disconnect();

        channel.connect("testgroup");
        view=channel.getView();
        System.out.println("-- view for channel: " + view);

        assert view.size() == 2;
        assert view.containsMember(channel.getLocalAddress());
        assert view.containsMember(coordinator.getLocalAddress());
        coordinator.close();
        channel.close();
    }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two
     * members. Test case introduced before fixing pbcast.NAKACK
     * bug, which used to leave pbcast.NAKACK in a broken state after
     * DISCONNECT. Because of this problem, the channel couldn't be used to
     * multicast messages.
     **/
    @Test
    public void testDisconnectConnectSendTwo() throws Exception {
        final Promise<Message> msgPromise=new Promise<Message>();
        Channel coordinator=createChannel("A");
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));
        coordinator.connect("testgroup");

        channel=createChannel("A");
        channel.connect("testgroup1");
        channel.disconnect();
        channel.connect("testgroup");
        channel.send(new Message(null, null, "payload"));
        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert msg.getObject().equals("payload");
        coordinator.close();
        channel.close();
    }







    private static class PromisedMessageListener extends ReceiverAdapter {
        private final Promise<Message> promise;

        public PromisedMessageListener(Promise<Message> promise) {
            this.promise=promise;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }
    }




}
