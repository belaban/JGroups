
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.jgroups.util.UUID;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Runs through multiple channel connect and disconnects, without closing the channel.
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class ConnectTest extends ChannelTestBase {
    JChannel channel, coordinator;


    @AfterMethod
    void tearDown() throws Exception {
        Util.close(channel, coordinator);
    }



    @Test
    public void testConnectAndDisconnect() throws Exception {
        channel=createChannel(true);
        final String GROUP=getUniqueClusterName("ConnectTest");
        for(int i=0; i < 5; i++) {
            System.out.print("Attempt #" + (i + 1));
            channel.connect(GROUP);
            System.out.println(": OK");
            channel.disconnect();
        }
    }

    @Test
    public void testDisconnectConnectOne() throws Exception {
        channel=createChannel(true);
        changeProps(channel);
        channel.connect("ConnectTest.testgroup-1");
        channel.disconnect();
        channel.connect("ConnectTest.testgroup-2");
        View view=channel.getView();
        assert view.size() == 1;
        assert view.containsMember(channel.getAddress());
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     **/
    @Test
    public void testDisconnectConnectTwo() throws Exception {
        View     view;
        coordinator=createChannel(true);
        changeProps(coordinator);
        coordinator.connect("ConnectTest.testgroup-3");
        print(coordinator, "coordinator");
        view=coordinator.getView();
        System.out.println("-- view for coordinator: " + view);
        assert view.size() == 1;

        channel=createChannel(coordinator);
        changeProps(channel);
        channel.connect("ConnectTest.testgroup-4");
        print(channel, "channel");
        view=channel.getView();
        System.out.println("-- view for channel: " + view);
        assert view.size() == 1;

        channel.disconnect();

        channel.connect("ConnectTest.testgroup-3");
        print(channel, "channel");
        view=channel.getView();
        System.out.println("-- view for channel: " + view);

        assert view.size() == 2;
        assert view.containsMember(channel.getAddress());
        assert view.containsMember(coordinator.getAddress());
    }


    static void print(JChannel ch, String msg) {
        System.out.println(msg + ": name=" + ch.getName() + ", addr=" + ch.getAddress() +
                ", UUID=" + ch.getAddressAsUUID() + "\nUUID cache:\n" + UUID.printCache() +
                "\nLogical_addr_cache:\n" + ch.getProtocolStack().getTransport().printLogicalAddressCache());
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
        coordinator=createChannel(true);
        changeProps(coordinator);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));
        coordinator.connect("ConnectTest.testgroup-5");

        channel=createChannel(coordinator);
        changeProps(channel);
        channel.connect("ConnectTest.testgroup-6");
        channel.disconnect();
        channel.connect("ConnectTest.testgroup-5");
        channel.send(new Message(null, null, "payload"));
        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert msg.getObject().equals("payload");
    }


    private static void changeProps(Channel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        transport.setLogDiscardMessages(false);
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
