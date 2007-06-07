
package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.util.Util;
import org.jgroups.stack.GossipRouter;

import java.util.Vector;
import java.util.List;
import java.util.LinkedList;


/**
 * Tests merging
 * @author Bela Ban
 * @version $Id: MergeTest.java,v 1.12 2007/06/07 10:42:12 belaban Exp $
 */
public class MergeTest extends TestCase {
    JChannel     channel;
    static final int    TIMES=10;
    static final int    router_port=12001;
    static final String bind_addr="127.0.0.1";
    GossipRouter router;
    JChannel     ch1, ch2;
    private ViewChecker  checker1, checker2;
    private static final int  NUM_MCASTS=5;
    private static final int  NUM_UCASTS=10;
    private static final long WAIT_TIME=2000L;

    String props="tunnel.xml";



    public MergeTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        startRouter();
        ch1=new JChannel(props);
        checker1=new ViewChecker(ch1);
        ch1.setReceiver(checker1);
        ch1.connect("demo");
        ch2=new JChannel(props);
        checker2=new ViewChecker(ch2);
        ch2.setReceiver(checker2);
        ch2.connect("demo");
        Util.sleep(1000);
    }

    public void tearDown() throws Exception {
        super.tearDown();
        ch2.close();
        ch1.close();
        stopRouter();
    }

    public void testPartitionAndSubsequentMerge() throws Exception {
        partitionAndMerge();
    }


    public void testTwoMerges() throws Exception {
        partitionAndMerge();
        partitionAndMerge();
    }



    private void partitionAndMerge() throws Exception {
        View v=ch2.getView();
        System.out.println("view is " + v);
        assertEquals("channel is supposed to have 2 members", 2, ch2.getView().size());

        System.out.println("sending " + NUM_MCASTS + " multicast messages");
        for(int i=0; i < NUM_MCASTS; i++) {
            Message msg=new Message();
            ch1.send(msg);
        }
        System.out.println("sending " + NUM_UCASTS + " unicast messages to " + v.size() + " members");
        Vector<Address> mbrs=v.getMembers();
        for(Address mbr: mbrs) {
            for(int i=0; i < NUM_UCASTS; i++) {
                Channel ch=i % 2 == 0? ch1 : ch2;
                ch.send(new Message(mbr));
            }
        }
        System.out.println("done, sleeping for " + WAIT_TIME + " time");
        Util.sleep(WAIT_TIME);

        System.out.println("++ simulating network partition by stopping the GossipRouter");
        stopRouter();

        System.out.println("sleeping for 10 secs");
        checker1.waitForNViews(1, 10000);
        checker2.waitForNViews(1, 10000);
        v=ch1.getView();
        System.out.println("-- ch1.view: " + v);

        v=ch2.getView();
        System.out.println("-- ch2.view: " + v);
        assertEquals("view should be 1 (channels should have excluded each other): " + v, 1, v.size());

        System.out.println("++ simulating merge by starting the GossipRouter again");
        startRouter();

        System.out.println("sleeping for 30 secs");
        checker1.waitForNViews(1, 30000);
        checker2.waitForNViews(1, 30000);

        v=ch1.getView();
        System.out.println("-- ch1.view: " + v);

        v=ch2.getView();
        System.out.println("-- ch2.view: " + v);

        assertEquals("channel is supposed to have 2 members again after merge", 2, ch2.getView().size());
    }






    private void startRouter() throws Exception {
        router=new GossipRouter(router_port, bind_addr);
        router.start();
    }

    private void stopRouter() {
        router.stop();
    }

    private static class ViewChecker extends ReceiverAdapter {
        final Object    mutex=new Object();
        int             count=0;
        final Channel   channel;
        final List<View> views=new LinkedList<View>();


        public ViewChecker(Channel channel) {
            this.channel=channel;
        }

        public void viewAccepted(View new_view) {
            synchronized(mutex) {
                count++;
                View view=channel != null? channel.getView() : null;
                views.add(view);
                // System.out.println("-- view: " + new_view + " (count=" + count + ", channel's view=" + view + ")");
                mutex.notifyAll();
            }
        }


        public void waitForNViews(int n, long timeout) {
            long sleep_time=timeout, curr, start;
            synchronized(mutex) {
                views.clear();
                count=0;
                start=System.currentTimeMillis();
                while(count < n) {
                    try {mutex.wait(sleep_time);} catch(InterruptedException e) {}
                    curr=System.currentTimeMillis();
                    sleep_time-=(curr - start);
                    if(sleep_time <= 0)
                        break;
                }
            }

            // System.out.println("+++++ VIEW_CHECKER for " + channel.getLocalAddress() + " terminated, view=" + channel.getView() +
            // ", views=" + views + ")");
        }


        public void receive(Message msg) {
            Address sender=msg.getSrc(), receiver=msg.getDest();
            boolean multicast=receiver == null || receiver.isMulticastAddress();
            System.out.println("[" + receiver + "]: received " + (multicast? " multicast " : " unicast ") + " message from " + sender);
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={MergeTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


}
