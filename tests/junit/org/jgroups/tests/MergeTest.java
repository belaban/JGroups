
package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.stack.GossipRouter;


/**
 * Tests merging
 * @author Bela Ban
 * @version $Id: MergeTest.java,v 1.6 2005/12/08 12:52:08 belaban Exp $
 */
public class MergeTest extends TestCase {
    JChannel     channel;
    final int    TIMES=10;
    final int    router_port=12000;
    final String bind_addr="127.0.0.1";
    GossipRouter router;
    JChannel     ch1, ch2;
    ViewChecker  checker;

    String props="TUNNEL(router_port=" + router_port + ";router_host=" +bind_addr+ ";loopback=true):" +
            "PING(timeout=1000;num_initial_members=2;gossip_host=" +bind_addr+";gossip_port=" + router_port + "):" +
            "MERGE2(min_interval=3000;max_interval=5000):" +
            "FD(timeout=1000;max_tries=2;shun=false):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=false;shun=false)";



    public MergeTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        startRouter();
        checker=new ViewChecker();
        ch1=new JChannel(props);
        ch1.setReceiver(checker);
        ch1.connect("demo");
        ch2=new JChannel(props);
        ch2.setReceiver(checker);
        ch2.connect("demo");
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

        System.out.println("++ simulating network partition by stopping the GossipRouter");
        stopRouter();

        System.out.println("sleeping for 10 secs");
        // Util.sleep(10000);
        checker.waitForNViews(2, 10000);

        v=ch1.getView();
        System.out.println("-- ch1.view: " + v);

        v=ch2.getView();
        System.out.println("-- ch2.view: " + v);
        assertEquals("view should be 1 (channels should have excluded each other", 1, v.size());

        System.out.println("++ simulating merge by starting the GossipRouter again");
        startRouter();

        System.out.println("sleeping for 30 secs");
        // Util.sleep(30000);
        checker.waitForNViews(2, 30000);

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

    private class ViewChecker extends ReceiverAdapter {
        final Object mutex=new Object();
        int          count=0;

        public void viewAccepted(View new_view) {
            synchronized(mutex) {
                count++;
                System.out.println("-- view: " + new_view);
                mutex.notifyAll();
            }
        }


        public void waitForNViews(int n, long timeout) {
            long sleep_time=timeout, curr, start;
            synchronized(mutex) {
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
        }
    }


    public static void main(String[] args) {
        String[] testCaseName={MergeTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


}
