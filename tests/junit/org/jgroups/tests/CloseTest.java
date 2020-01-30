
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Demos the creation of a channel and subsequent connection and closing. Demo application should exit (no
 * more threads running)
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class CloseTest extends ChannelTestBase {
    protected JChannel a, b, c;

    @AfterMethod void tearDown() throws Exception {Util.close(c,b,a);}


    public void testDoubleClose() throws Exception {
        a=createChannel(true, 1, "A");
        a.connect("CloseTest.testDoubleClose");
        assert a.isOpen();
        assert a.isConnected();
        Util.close(a);
        Util.close(a);
        assert !a.isConnected();
    }

    public void testCreationAndClose() throws Exception {
        a=createChannel(true, 1, "A");
        a.connect("CloseTest.testCreationAndClose");
        assert a.isOpen();
        Util.close(a);
        assert !a.isConnected();
    }

    public void testCreationAndCoordClose() throws Exception {
        a=createChannel(true, 2, "A");
        b=createChannel(a, "B");
        a.connect("testCreationAndCoordClose");
        b.connect("testCreationAndCoordClose");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        a.disconnect();
        Util.waitUntilAllChannelsHaveSameView(10000, 500, b);
    }

    public void testViewChangeReceptionOnChannelCloseByParticipant() throws Exception {
        List<Address> members;
        MyReceiver    r1=new MyReceiver(), r2=new MyReceiver();
        Address       a_addr, b_addr;
        final String  GROUP="CloseTest.testViewChangeReceptionOnChannelCloseByParticipant";

        a=createChannel(true, 2, "A");
        a.setReceiver(r1);
        a.connect(GROUP);
        System.out.println("A: " + r1.getViews());

        b=createChannel(a, "B");
        b.setReceiver(r2);
        r1.clearViews();
        b.connect(GROUP);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
        a_addr=a.getAddress();
        b_addr=b.getAddress();

        Util.close(b);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a);
        View v=r1.getViews().get(0);
        members=v.getMembers();
        System.out.println("-- first view of c1: " + v);
        Assert.assertEquals(2, members.size());
        assertTrue(members.contains(a_addr));
        assertTrue(members.contains(b_addr));

        v=r1.getViews().get(1);
        members=v.getMembers();
        System.out.println("-- second view of c1: " + v);
        assert 1 == members.size();
        assert members.contains(a_addr);
        assert !members.contains(b_addr);
    }

    public void testViewChangeReceptionOnChannelCloseByCoordinator() throws Exception {
        List<Address> members;
        MyReceiver    r1=new MyReceiver(), r2=new MyReceiver();
        Address       a_addr, b_addr;

        final String GROUP="CloseTest.testViewChangeReceptionOnChannelCloseByCoordinator";
        a=createChannel(true, 2, "A");
        a.setReceiver(r1);
        a.connect(GROUP);
        b=createChannel(a, "B");
        b.setReceiver(r2);
        b.connect(GROUP);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
        a_addr=a.getAddress();
        b_addr=b.getAddress();
        View v=r2.getViews().get(0);
        members=v.getMembers();
        assert 2 == members.size();
        assert members.contains(a.getAddress());

        r2.clearViews();
        Util.close(b);
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a);

        v=r1.getViews().get(r1.getViews().size() -1);
        members=v.getMembers();
        assert 1 == members.size();
        assert members.contains(a_addr);
        assert !members.contains(b_addr);
    }

    public void testConnectDisconnectConnectCloseSequence() throws Exception {
        a=createChannel(true, 1, "A");

        a.connect("CloseTest.testConnectDisconnectConnectCloseSequence-CloseTest");
        System.out.println("view is " + a.getView());

        System.out.println("-- disconnecting channel --");
        a.disconnect();

        System.out.println("-- connecting channel to OtherGroup --");
        a.connect("CloseTest.testConnectDisconnectConnectCloseSequence-OtherGroup");
        System.out.println("view is " + a.getView());
    }


    public void testConnectCloseSequenceWith2Members() throws Exception {
        a=createChannel(true, 2, "A");
        final String GROUP="CloseTest.testConnectCloseSequenceWith2Members";
        a.connect(GROUP);

        b=createChannel(a, "B");
        b.connect(GROUP);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
        System.out.println("view is " + b.getView());
    }

    public void testCreationAndClose2() throws Exception {
        a=createChannel(true, 1, "A");
        a.connect("CloseTest.testCreationAndClose2");
    }

    public void testClosedChannel() throws Exception {
        a=createChannel(true, 1, "A");
        a.connect("CloseTest.testClosedChannel");
        Util.close(a);
        Util.sleep(2000);
        try {
            a.connect("CloseTest.testClosedChannel");
            assert false;
        }
        catch(IllegalStateException ex) {
        }
    }

   
    public void testMultipleConnectsAndDisconnects() throws Exception {
        final String GROUP="CloseTest.testMultipleConnectsAndDisconnects";
        a=createChannel(true, 10, "A");
        assert a.isOpen() && !a.isConnected();
        a.connect(GROUP);
        assert a.isConnected();
        assertView(a, 1);
        // printViews(a);

        b=createChannel(a, "B");
        assert b.isOpen() && !b.isConnected();
        System.out.println("-- B joining");
        b.connect(GROUP);
        assert b.isConnected();
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
        // printViews(a,b);

        System.out.println("-- B leaving");
        b.disconnect();
        assert b.isOpen() && !b.isConnected();
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a);
        // printViews(a);

        System.out.println("-- B joining");
        b.connect(GROUP);
        assert b.isConnected();
        Util.waitUntilAllChannelsHaveSameView(20000, 1000, a, b);

        // Now see what happens if we disaconnect and reconnect A (the current coord)
        System.out.println("-- A leaving");
        a.disconnect();
        assert a.isOpen() && !a.isConnected();
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, b);
        printViews(b);

        System.out.println("-- A joining");
        a.connect(GROUP);
        assert a.isOpen() && a.isConnected();
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);
        // printViews(a,b);
    }



    public void testMultipleConnectsAndDisconnects2() throws Exception {
        int NUM=10;
        a=createChannel(true, NUM, "A");
        a.connect("CloseTest");
        b=createChannel(a, "B");
        b.connect("CloseTest");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        for(int i=1; i <= NUM; i++) {
            System.out.print("#" + i + " disconnecting: ");
            b.disconnect();
            System.out.println("OK");
            Util.waitUntilAllChannelsHaveSameView(10000, 500, a);
            b.connect("CloseTest");
            Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);
        }
    }


    private static void assertView(JChannel ch, int num) {
        View view=ch.getView();
        String msg="view=" + view;
        assertNotNull(view);
        Assert.assertEquals(view.size(), num, msg);
    }

    protected void printViews(JChannel ... channels) {
        System.out.printf("views:\n%s\n",
                          Stream.of(channels).map(ch -> ch.getAddress() + ": " + ch.getView().toString())
                            .collect(Collectors.joining("\n")));
    }


    private static class MyReceiver implements Receiver {
        final List<View> views=new ArrayList<>();
        public void viewAccepted(View new_view) {
            views.add(new_view);
            System.out.println("new_view = " + new_view);
        }
        public List<View> getViews() {return views;}
        public void clearViews() {views.clear();}
    }

}
