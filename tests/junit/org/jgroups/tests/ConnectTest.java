
package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.NameCache;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;


/**
 * Runs through multiple channel connect and disconnects, without closing the channel.
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class ConnectTest extends ChannelTestBase {
    protected JChannel a, b;


    @AfterMethod void tearDown() throws Exception {
        Util.close(b, a);
    }


    public void testConnectAndDisconnect() throws Exception {
        b=createChannel();
        makeUnique(b);
        final String GROUP=ConnectTest.class.getSimpleName();
        for(int i=0; i < 5; i++) {
            System.out.print("Attempt #" + (i + 1));
            b.connect(GROUP);
            System.out.println(": OK");
            b.disconnect();
        }
    }


    public void testDisconnectConnectOne() throws Exception {
        b=createChannel();
        makeUnique(b);
        changeProps(b);
        b.connect("ConnectTest.testgroup-1");
        b.disconnect();
        b.connect("ConnectTest.testgroup-2");
        View view=b.getView();
        assert view.size() == 1;
        assert view.containsMember(b.getAddress());
    }


    /**
     * Tests connect-disconnect-connect sequence for a group with two members
     **/
    public void testDisconnectConnectTwo() throws Exception {
        a=createChannel().name("A");
        changeProps(a);

        b=createChannel().name("B");
        changeProps(b);
        makeUnique(a, b);

        a.connect("ConnectTest.testgroup-3");
        print(a, "coord");
        View view=a.getView();
        System.out.println("-- view for coordinator: " + view);
        assert view.size() == 1;

        b.connect("ConnectTest.testgroup-4");
        print(b, "channel");
        view=b.getView();
        System.out.println("-- view for channel: " + view);
        assert view.size() == 1;

        b.disconnect();

        b.connect("ConnectTest.testgroup-3");
        print(b, "channel");

        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
        view=b.getView();
        System.out.println("-- view for channel: " + view);

        assert view.size() == 2;
        assert view.containsMember(b.getAddress());
        assert view.containsMember(a.getAddress());
    }


    // @Test(invocationCount=10)
    public void testMultipleConnectsAndDisconnects() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        makeUnique(a, b);
        for(JChannel c: Arrays.asList(a, b))
            ((GMS)c.getProtocolStack().findProtocol(GMS.class)).printLocalAddress(false);
        a.connect("testMultipleConnectsAndDisconnects");
        b.connect("testMultipleConnectsAndDisconnects");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        for(int i=1; i <= 50; i++) {
            b.disconnect();
            Util.waitUntil(5000, 500, () -> a.getView().size() == 1);
            assert a.getView().size() == 1 : String.format("coord's view is %s\n", a.getView());
            assert b.isConnected() == false;
            b.connect("testMultipleConnectsAndDisconnects");
            Util.waitUntilAllChannelsHaveSameView(5000, 500, a, b);
            System.out.printf("#%d: %s\n", i, a.getView());
        }
    }

    public void testDisconnectConnectedMessageSending() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        makeUnique(a, b);
        a.connect("ConnectTest");
        b.connect("ConnectTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);

        MyReceiver<Integer> receiver=new MyReceiver<>();
        b.setReceiver(receiver);

        for(int i=1; i <= 5; i++) {
            a.send(new BytesMessage(b.getAddress(), i));
            a.send(new BytesMessage(null, i+5));
        }
        List<Integer> list=receiver.list();
        Util.waitUntilListHasSize(list, 10, 5000, 500);
        System.out.println("list = " + list);

        list.clear();
        b.disconnect();

        b.connect("ConnectTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);

        for(int i=1; i <= 5; i++) {
            a.send(new BytesMessage(b.getAddress(), i));
            a.send(new BytesMessage(null, i+5));
        }
        Util.waitUntilListHasSize(list, 10, 5000, 500);
        System.out.println("list = " + list);
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
        a=createChannel();
        changeProps(a);
        a.setReceiver(new PromisedMessageListener(msgPromise));
        b=createChannel();
        changeProps(b);
        makeUnique(a, b);
        a.connect("ConnectTest.testgroup-5");
        b.connect("ConnectTest.testgroup-6");
        b.disconnect();
        b.connect("ConnectTest.testgroup-5");
        b.send(new BytesMessage(null, "payload"));
        Message msg=msgPromise.getResult(20000);
        assert msg != null;
        assert msg.getObject().equals("payload");
    }


    private static void changeProps(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        TP transport=stack.getTransport();
        transport.setLogDiscardMessages(false);
    }




    private static class PromisedMessageListener implements Receiver {
        private final Promise<Message> promise;

        public PromisedMessageListener(Promise<Message> promise) {
            this.promise=promise;
        }

        public void receive(Message msg) {
            promise.setResult(msg);
        }
    }




}
