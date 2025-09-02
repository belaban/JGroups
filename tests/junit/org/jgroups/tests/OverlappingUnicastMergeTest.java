package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.TCP;
import org.jgroups.protocols.TP;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Tests overlapping merges, e.g. A: {A,B}, B: {A,B} and C: {A,B,C}. Tests unicast tables<p>
 * Related JIRA: https://issues.redhat.com/browse/JGRP-940
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class OverlappingUnicastMergeTest extends ChannelTestBase {
    private JChannel a, b, c;
    private MyReceiver ra, rb, rc;

    @BeforeMethod
    void start() throws Exception {
        ra=new MyReceiver("A"); rb=new MyReceiver("B"); rc=new MyReceiver("C");
        a=createChannel().name("A").setReceiver(ra);
        b=createChannel().name("B").setReceiver(rb);
        c=createChannel().name("C").setReceiver(rc);

        makeUnique(a,b,c);
        modifyConfigs(a, b, c);

        a.connect("OverlappingUnicastMergeTest");
        b.connect("OverlappingUnicastMergeTest");
        c.connect("OverlappingUnicastMergeTest");

        Util.waitUntilAllChannelsHaveSameView(5000, 500, a,b,c);
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c,b,a);
    }


    public void testWithAllViewsInSync() throws Exception {
        sendAndCheckMessages(5, a, b, c);
    }

    /**
     * Verifies that unicasts are received correctly by all participants after an overlapping merge. The following steps
     * are executed:
     * <ol>
     * <li>Group is {A,B,C}, A is the coordinator</li>
     * <li>MERGE3 is removed from all members</li>
     * <li>VERIFY_SUSPECT is removed from all members</li>
     * <li>Everyone sends 5 unicast messages to everyone else</li>
     * <li>A VIEW(B,C) is injected into B and C</li>
     * <li>B and C install {B,C}</li>
     * <li>B and C trash the connection table for A in UNICAST</li>
     * <li>A still has view {A,B,C} and all connection tables intact in UNICAST</li>
     * <li>We now send N unicasts from everyone to everyone else, all the unicasts should be received.</li>
     * </ol>
     */
    public void testWithViewBC() throws Exception {

        System.out.println("A's view: " + a.getView());

        // Inject view {B,C} into B and C:
        View new_view=View.create(b.getAddress(), 10, b.getAddress(), c.getAddress());
        injectView(new_view, b, c);
        assert 3 == a.getView().size();
        assert 2 == b.getView().size();
        assert 2 == c.getView().size();
        sendAndCheckMessages(5, a, b, c);
    }

    public void testWithViewA() throws Exception {
        // Inject view {A} into A, B and C:
        View new_view=View.create(a.getAddress(), 10, a.getAddress());
        injectView(new_view, a, b, c);
        sendAndCheckMessages(5, a, b, c);
    }

    public void testWithViewC() throws Exception {
        // Inject view {A} into A, B and C:
        View new_view=View.create(c.getAddress(), 10, c.getAddress());
        injectView(new_view, a, b, c);
        sendAndCheckMessages(5, a, b, c);
    }

    public void testWithEveryoneHavingASingletonView() throws Exception {
        // Inject view {A} into A, {B} into B and {C} into C, then send unicasts: unicast messages should be
        // received despite the members not being in the same view
        for(JChannel ch: List.of(a,b,c))
            injectView(View.create(ch.getAddress(), 10, ch.getAddress()), ch);
        if(a.getProtocolStack().findProtocol(TCP.class) != null)
            Util.waitUntil(5000, 500, () -> getNumberOfConnections(a, b, c) == 0,
                           () -> String.format("number of connections != 0: %s\n", printConnections(a, b, c)));
        sendAndCheckMessages(5, a, b, c);
    }

    protected static String printConnections(JChannel... channels) {
        StringBuilder sb=new StringBuilder();
        for(JChannel ch: channels) {
            TCP tcp=(TCP)ch.getProtocolStack().getTransport();
            sb.append(String.format("%s: %s\n", ch.getAddress(), tcp.printConnections()));
        }
        return sb.toString();
    }

    protected static int getNumberOfConnections(JChannel... channels) {
        int retval=0;
        for(JChannel ch: channels) {
            TP tp=ch.getProtocolStack().getTransport();
            if(tp instanceof TCP) {
                retval+=((TCP)tp).getOpenConnections();
            }
            else
                retval--;
        }
        return retval;
    }


    private static void injectView(View view, JChannel ... channels) {
        for(JChannel ch: channels) {
            ch.down(new Event(Event.VIEW_CHANGE, view));
            ch.up(new Event(Event.VIEW_CHANGE, view));
        }
        for(JChannel ch: channels) {
            MyReceiver receiver=(MyReceiver)ch.getReceiver();
            System.out.println("[" + receiver.name + "] view=" + ch.getView());
        }
    }


    private void sendAndCheckMessages(int num_msgs, JChannel ... channels) throws Exception {
        ra.clear(); rb.clear(); rc.clear();
        // 1. send unicast messages
        List<Address> mbrs=new ArrayList<>(channels.length);
        for(JChannel ch: channels)
            mbrs.add(ch.getAddress());

        for(JChannel ch: channels) {
            Address addr=ch.getAddress();
            for(Address dest: mbrs) {
                for(int i=1; i <= num_msgs; i++)
                    ch.send(dest, String.format("%s%d", addr, i));
            }
        }
        int total_msgs=num_msgs * channels.length;
        MyReceiver[] receivers=new MyReceiver[channels.length];
        for(int i=0; i < channels.length; i++)
            receivers[i]=(MyReceiver)channels[i].getReceiver();
        checkReceivedMessages(total_msgs, receivers);
    }

    private static void checkReceivedMessages(int num_ucasts, MyReceiver ... receivers) {
        for(int i=0; i < 20; i++) {
            boolean all_received=true;
            for(MyReceiver receiver: receivers) {
                List<Message> ucasts=receiver.getUnicasts();
                int ucasts_received=ucasts.size();
                if(num_ucasts != ucasts_received) {
                    all_received=false;
                    break;
                }
            }
            if(all_received)
                break;
            Util.sleep(500);
        }

        for(MyReceiver receiver: receivers) {
            List<Message> ucasts=receiver.getUnicasts();
            int ucasts_received=ucasts.size();
            System.out.println("receiver " + receiver + ": ucasts=" + ucasts_received);
        }
        for(MyReceiver receiver: receivers) {
            List<Message> ucasts=receiver.getUnicasts();
            int ucasts_received=ucasts.size();
            assert num_ucasts == ucasts_received : "ucasts for " + receiver + ": " + print(ucasts);
        }
    }

    static String print(List<Message> list) {
        return list.stream().map(m -> (String)m.getObject()).collect(Collectors.joining(", "));
    }

    private static void modifyConfigs(JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            stack.removeProtocols("MERGE3", "VERIFY_SUSPECT", "FC", "UFC", "MFC");
        }
    }



    private static class MyReceiver implements Receiver {
        final String name;
        final List<Message> ucasts=new ArrayList<>(20);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            Address dest=msg.getDest();
            boolean mcast=dest == null;
            if(!mcast) {
                synchronized(ucasts) {
                    ucasts.add(msg);
                }
            }
        }

        public List<Message> getUnicasts() { return ucasts; }
        public void clear() {ucasts.clear();}
        public String toString() {
            return name;
        }
    }



}