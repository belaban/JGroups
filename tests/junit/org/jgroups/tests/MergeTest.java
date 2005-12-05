
package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.util.Util;
import org.jgroups.stack.GossipRouter;


/**
 * Tests merging
 * @author Bela Ban
 * @version $Id: MergeTest.java,v 1.2 2005/12/05 16:31:04 belaban Exp $
 */
public class MergeTest extends TestCase {
    JChannel     channel;
    final int    TIMES=10;
    final int    router_port=12000;
    GossipRouter router;
    JChannel     ch1, ch2;

    String props="TUNNEL(router_port=" + router_port + ";router_host=127.0.0.1;loopback=true):" +
            "PING(timeout=1000;num_initial_members=2;gossip_host=127.0.0.1;gossip_port=" + router_port + "):" +
            "MERGE2(min_interval=7000;max_interval=10000):" +
            "FD(timeout=2000;max_tries=2):" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=false)";




    public MergeTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        startRouter();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        stopRouter();
    }

    public void testPartitionAndSubsequentMerge() throws Exception {
        ch1=new JChannel(props);
        ch1.connect("demo");
        ch2=new JChannel(props);
        ch2.connect("demo");
        View v=ch2.getView();
        System.out.println("view is " + v);
        assertEquals("channel is supposed to have 2 members", 2, ch2.getView().size());

        System.out.println("++ simulating network partition by stopping the GossipRouter");
        stopRouter();

        System.out.println("sleeping for 10 secs");
        Util.sleep(10000);
        v=ch1.getView();
        System.out.println("-- ch1.view: " + v);

        v=ch2.getView();
        System.out.println("-- ch2.view: " + v);
        assertEquals("view should be 1 (channels should have excluded each other", 1, v.size());

        System.out.println("++ simulating merge by starting the GossipRouter again");
        startRouter();

        System.out.println("sleeping for 10 secs");
        Util.sleep(20000);

        v=ch1.getView();
        System.out.println("-- ch1.view: " + v);

        v=ch2.getView();
        System.out.println("-- ch2.view: " + v);

        assertEquals("channel is supposed to have 2 members again after merge", 2, ch2.getView().size());
    }


    private void startRouter() throws Exception {
        router=new GossipRouter(router_port, "127.0.0.1");
        router.start();
    }

    private void stopRouter() {
        router.stop();
    }



    public static void main(String[] args) {
        String[] testCaseName={MergeTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


}
