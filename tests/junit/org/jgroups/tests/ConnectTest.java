
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.NameCache;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;


/**
 * Runs through multiple channel connect and disconnects, without closing the channel.
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class ConnectTest extends ChannelTestBase {
    JChannel channel, coordinator;


    @AfterMethod void tearDown() throws Exception {
        Util.close(channel, coordinator);
    }


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
    public void testDisconnectConnectTwo() throws Exception {
        coordinator=createChannel(true, 3, "coord");
        changeProps(coordinator);
        coordinator.connect("ConnectTest.testgroup-3");
        print(coordinator, "coord");
        View view=coordinator.getView();
        System.out.println("-- view for coordinator: " + view);
        assert view.size() == 1;

        channel=createChannel(coordinator, "channel");
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


    protected static void print(JChannel ch, String msg) {
        System.out.println(msg + ": name=" + ch.getName() + ", addr=" + ch.getAddress() +
                             ", UUID=" + ch.getAddressAsUUID() + "\nUUID cache:\n" + NameCache.printCache() +
                             "\nLogical_addr_cache:\n" + ch.getProtocolStack().getTransport().printLogicalAddressCache());
    }


    /**
     * Tests connect-disconnect-connect-send sequence for a group with two members
     **/
    public void testDisconnectConnectSendTwo() throws Exception {
        final Promise<Message> msgPromise=new Promise<>();
        coordinator=createChannel(true);
        changeProps(coordinator);
        coordinator.setReceiver(new PromisedMessageListener(msgPromise));
        coordinator.connect("ConnectTest.testgroup-5");

        channel=createChannel(coordinator);
        changeProps(channel);
        channel.connect("ConnectTest.testgroup-6");
        channel.disconnect();
        channel.connect("ConnectTest.testgroup-5");
        channel.send(new Message(null, "payload"));
        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert msg.getObject().equals("payload");
    }


    private static void changeProps(JChannel ch) {
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
