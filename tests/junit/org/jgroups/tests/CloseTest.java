// $Id: CloseTest.java,v 1.3 2005/07/29 08:59:37 belaban Exp $

package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.ChannelClosedException;
import org.jgroups.JChannel;
import org.jgroups.ChannelException;
import org.jgroups.util.Util;


/**
 * Demos the creation of a channel and subsequent connection and closing. Demo application should exit (no
 * more threads running)
 */
public class CloseTest extends TestCase {
    JChannel channel, channel1, channel2;

    //String   props="UDP:PING(num_initial_members=2;timeout=3000):FD:" +
    //"pbcast.PBCAST(gossip_interval=5000;gc_lag=30):UNICAST:FRAG:" +
    //"pbcast.GMS";


    String props="UDP:PING(num_initial_members=2;timeout=3000):FD:" +
            "pbcast.PBCAST(gossip_interval=5000;gc_lag=30):UNICAST:FRAG:" +
            "pbcast.GMS";


    String props2="UDP(mcast_addr=228.8.8.8;mcast_port=45566;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            //"PIGGYBACK(max_wait_time=100;max_size=32000):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=1200,2400,4800):" +
            "UNICAST(timeout=1200):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=8096;down_thread=false;up_thread=false):" +
            // "CAUSAL:" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=true)";


    public CloseTest(String name) {
        super(name);
    }


    public void testCreationAndClose() throws Exception {
        System.out.println("-- creating channel1 --");
        channel1=new JChannel(props);
        System.out.println("-- connecting channel1 --");
        channel1.connect("CloseTest1");
        System.out.println("-- closing channel1 --");
        channel1.close();
        //Util.sleep(2000);
        System.out.println("-- done, threads are ");
        Util.printThreads();
    }


    public void testCreationAndClose2() throws Exception {
        System.out.println("-- creating channel2 --");
        channel2=new JChannel(props2);
        System.out.println("-- connecting channel2 --");
        channel2.connect("CloseTest2");
        System.out.println("-- closing channel --");
        channel2.close();
        Util.sleep(2000);
        //System.out.println("-- done, threads are ");
        Util.printThreads();
    }


    public void testChannelClosedException() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel(props2);
        System.out.println("-- connecting channel --");
        channel.connect("CloseTestLoop");
        System.out.println("-- closing channel --");
        channel.close();
        Util.sleep(2000);

        try {
            channel.connect("newGroup");
            fail(); // cannot connect to a closed channel
        }
        catch(ChannelClosedException ex) {
            assertTrue(true);
        }
    }

    public void testCreationAndCloseLoop() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel(props);

        for(int i=1; i <= 10; i++) {
            System.out.println("-- connecting channel (attempt #" + i + " ) --");
            channel.connect("CloseTestLoop2");
            System.out.println("-- closing channel --");
            channel.close();

            System.out.println("-- reopening channel --");
            channel.open();
        }
        channel.close();
    }

    public void testShutdown() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel(props);
        System.out.println("-- connecting channel --");
        channel.connect("bla");
        System.out.println("-- shutting down channel --");
        channel.shutdown();

        Thread threads[]=new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        System.out.println("-- active threads:");
        for(int i=0; i < threads.length; i++)
            System.out.println(threads[i]);
        assertTrue(threads.length < 5);
    }

    public static void main(String[] args) {
        String[] testCaseName={CloseTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
