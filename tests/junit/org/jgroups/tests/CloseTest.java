
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
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
        a=createChannel().name("A");
        makeUnique(a);
        a.connect("CloseTest.testDoubleClose");
        assert a.isOpen();
        assert a.isConnected();
        Util.close(a);
        Util.close(a);
        assert !a.isConnected();
    }

    public void testCreationAndClose() throws Exception {
        a=createChannel().name("A");
        makeUnique(a);
        a.connect("CloseTest.testCreationAndClose");
        assert a.isOpen();
        Util.close(a);
        assert !a.isConnected();
    }

    public void testCreationAndCoordClose() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        makeUnique(a,b);
        a.connect("testCreationAndCoordClose");
        b.connect("testCreationAndCoordClose");
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a, b);

        a.disconnect();
        Util.waitUntilAllChannelsHaveSameView(10000, 500, b);
    }

    public void testViewChangeReceptionOnChannelCloseByParticipant() throws Exception {
        List<Address> members;
        MyReceiver    r1=new MyReceiver(), r2=new MyReceiver();
        final String  GROUP="CloseTest.testViewChangeReceptionOnChannelCloseByParticipant";

        a=createChannel().name("A").setReceiver(r1);
        b=createChannel().name("B").setReceiver(r2);
        makeUnique(a,b);
        a.connect(GROUP);
        b.connect(GROUP);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a, b);

        Util.close(b);
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a);
        View v=r1.getViews().get(0);
        members=v.getMembers();
        System.out.println("-- first view of A: " + v);
        assert 1 == members.size();
        assert members.contains(a.getAddress());

        v=r1.getViews().get(1);
        members=v.getMembers();
        System.out.println("-- second view of A: " + v);
        assert 2 == members.size();

        v=r1.getViews().get(2);
        members=v.getMembers();
        System.out.println("-- third view of A: " + v);
        assert 1 == members.size();

    }

    public void testViewChangeReceptionOnChannelCloseByCoordinator() throws Exception {
        List<Address> members;
        MyReceiver    r1=new MyReceiver(), r2=new MyReceiver();
        Address       a_addr, b_addr;

        final String GROUP="CloseTest.testViewChangeReceptionOnChannelCloseByCoordinator";
        a=createChannel().name("A").setReceiver(r1);
        b=createChannel().name("B").setReceiver(r2);
        makeUnique(a,b);
        a.connect(GROUP);
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
        Util.waitUntil(10000, 500, () -> a.getView().size() == 1);

        v=r1.getViews().get(r1.getViews().size() -1);
        members=v.getMembers();
        assert 1 == members.size();
        assert members.contains(a_addr);
        assert !members.contains(b_addr);
    }

    public void testConnectDisconnectConnectCloseSequence() throws Exception {
        a=createChannel().name("A");
        makeUnique(a);
        a.connect("CloseTest.testConnectDisconnectConnectCloseSequence-CloseTest");
        System.out.println("view is " + a.getView());

        System.out.println("-- disconnecting channel --");
        a.disconnect();

        System.out.println("-- connecting channel to OtherGroup --");
        a.connect("CloseTest.testConnectDisconnectConnectCloseSequence-OtherGroup");
        System.out.println("view is " + a.getView());
    }


    public void testClosedChannel() throws Exception {
        a=createChannel().name("A");
        makeUnique(a);
        a.connect("CloseTest.testClosedChannel");
        Util.close(a);
        try {
            a.connect("CloseTest.testClosedChannel");
            assert false;
        }
        catch(IllegalStateException ex) {
        }
    }

   
    public void testMultipleConnectsAndDisconnects() throws Exception {
        final String GROUP="CloseTest.testMultipleConnectsAndDisconnects";
        a=createChannel().name("A");
        assert a.isOpen() && !a.isConnected();
        b=createChannel().name("B");
        assert b.isOpen() && !b.isConnected();
        makeUnique(a,b);

        a.connect(GROUP);
        assert a.isConnected();
        assertView(a, 1);

        System.out.println("-- B joining");
        b.connect(GROUP);
        assert b.isConnected();
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a, b);

        System.out.println("-- B leaving");
        b.disconnect();
        assert b.isOpen() && !b.isConnected();
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a);

        System.out.println("-- B joining");
        b.connect(GROUP);
        assert b.isConnected();
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a, b);

        // Now see what happens if we disaconnect and reconnect A (the current coord)
        System.out.println("-- A leaving");
        a.disconnect();
        assert a.isOpen() && !a.isConnected();
        Util.waitUntilAllChannelsHaveSameView(5000, 500, b);
        printViews(b);

        System.out.println("-- A joining");
        a.connect(GROUP);
        assert a.isOpen() && a.isConnected();
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a, b);
    }


    public void testMultipleConnectsAndDisconnects2() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        makeUnique(a,b);
        a.connect("CloseTest");
        b.connect("CloseTest");
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a, b);

        for(int i=1; i <= 10; i++) {
            System.out.print("#" + i + " disconnecting: ");
            b.disconnect();
            System.out.println("OK");
            Util.waitUntilAllChannelsHaveSameView(5000, 500, a);
            b.connect("CloseTest");
            Util.waitUntilAllChannelsHaveSameView(5000, 500, a, b);
        }
    }

    public void testAlternatingCoordAndParticipantDisconnects() throws Exception {
        a=createChannel().name("A");
        b=createChannel().name("B");
        makeUnique(a,b);
        a.connect("CloseTest");
        b.connect("CloseTest");
        Util.waitUntilAllChannelsHaveSameView(5000, 500, a, b);

        for(int i=1; i <= 10; i++) {
            JChannel ch=i % 2 == 0? a : b;
            leaveAndRejoin(i, ch, a,b);
        }
    }

    private static void leaveAndRejoin(int i, JChannel ch, JChannel... channels) throws Exception {
        System.out.printf("#%d disconnecting %s, view is %s ", i, ch.getName(), ch.getView());
        ch.disconnect();
        System.out.println("OK");

        BooleanSupplier p=() -> Stream.of(channels)
          .allMatch(c -> !c.isConnected() || c.isConnected() && c.getView().size() == 1);
        Supplier<String> message=() -> Stream.of(channels)
          .map(c -> String.format("%s: connected=%b view=%s", c.getAddress(), c.isConnected(), c.getView()))
          .collect(Collectors.joining("\n"));

        // one channel must be disconnected and the other must have a view of 1
        Util.waitUntil(5000, 500, p, message);

        System.out.printf("#%d rejoining %s: ", i, ch.getName());
        ch.connect("CloseTest");
        Util.waitUntilAllChannelsHaveSameView(5000, 500, channels);
        System.out.printf("OK, view is %s\n", ch.getView());
    }


    private static void assertView(JChannel ch, int num) {
        View view=ch.getView();
        String msg="view=" + view;
        assert view != null;
        Assert.assertEquals(view.size(), num, msg);
    }

    protected static void printViews(JChannel... channels) {
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
