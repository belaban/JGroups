// $Id: ConnectStressTest.java,v 1.15 2005/10/27 08:32:33 belaban Exp $

package org.jgroups.tests;


import EDU.oswego.cs.dl.util.concurrent.BrokenBarrierException;
import EDU.oswego.cs.dl.util.concurrent.CyclicBarrier;
import junit.framework.TestCase;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Vector;


/**
 * Creates 1 channel, then creates NUM channels, all try to join the same channel concurrently.
 * @author Bela Ban Nov 20 2003
 * @version $Id: ConnectStressTest.java,v 1.15 2005/10/27 08:32:33 belaban Exp $
 */
public class ConnectStressTest extends TestCase {
    static CyclicBarrier  start_connecting=null;
    static CyclicBarrier  connected=null;
    static CyclicBarrier  received_all_views=null;
    static CyclicBarrier  start_disconnecting=null;
    static CyclicBarrier  disconnected=null;
    static final int      NUM=30;
    static final MyThread[] threads=new MyThread[NUM];
    static JChannel       channel;
    static String         groupname="ConcurrentTestDemo";


    static String props="UDP(mcast_addr=228.8.8.9;mcast_port=7788;ip_ttl=1;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=3000;num_initial_members=10):" +
            "MERGE2(min_interval=3000;max_interval=5000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=300,600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=5000):" +
            "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=false;print_local_addr=false;view_ack_collection_timeout=5000;" +
            "digest_timeout=0;merge_timeout=30000)";



    public ConnectStressTest(String name) {
        super(name);

    }


    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }


    public void testConcurrentJoins() throws Exception {
        start_connecting=new CyclicBarrier(NUM +1);
        connected=new CyclicBarrier(NUM +1);
        received_all_views=new CyclicBarrier(NUM +1);
        start_disconnecting=new CyclicBarrier(NUM +1);
        disconnected=new CyclicBarrier(NUM +1);

        long start, stop;

        //  create main channel - will be coordinator for JOIN requests
        channel=new JChannel(props);
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        start=System.currentTimeMillis();
        channel.connect(groupname);
        stop=System.currentTimeMillis();
        log(channel.getLocalAddress() + " connected in " + (stop-start) + " msecs (" +
                    channel.getView().getMembers().size() + " members). VID=" + channel.getView().getVid());
        assertEquals(channel.getView().getMembers().size(), 1);

        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(i);
            threads[i].start();
        }

        // signal the threads to start connecting to their channels
        start_connecting.barrier();
        start=System.currentTimeMillis();

        try {
            connected.barrier();
            stop=System.currentTimeMillis();
            System.out.println("-- took " + (stop-start) + " msecs for all " + NUM + " threads to connect");

            received_all_views.barrier();
            stop=System.currentTimeMillis();
            System.out.println("-- took " + (stop-start) + " msecs for all " + NUM + " threads to see all views");

            int num_members=-1;
            for(int i=0; i < 10; i++) {
                View v=channel.getView();
                num_members=v.getMembers().size();
                System.out.println("*--* number of members connected: " + num_members + ", (expected: " +(NUM+1) +
                        "), v=" + v);
                if(num_members == NUM+1)
                    break;
                Util.sleep(500);
            }
            assertEquals((NUM+1), num_members);
        }
        catch(Exception ex) {
            fail(ex.toString());
        }
    }

    public void testConcurrentLeaves() throws Exception {
        start_disconnecting.barrier();
        long start, stop;
        start=System.currentTimeMillis();

        disconnected.barrier();
        stop=System.currentTimeMillis();
        System.out.println("-- took " + (stop-start) + " msecs for " + NUM + " threads to disconnect");

        int num_members=0;
        for(int i=0; i < 10; i++) {
            View v=channel.getView();
            Vector mbrs=v != null? v.getMembers() : null;
            if(mbrs != null) {
                num_members=mbrs.size();
                System.out.println("*--* number of members connected: " + num_members + ", (expected: 1), view=" + v);
                if(num_members <= 1)
                    break;
            }
            Util.sleep(3000);
        }
        assertEquals(1, num_members);
        log("closing all channels");
        for(int i=0; i < threads.length; i++) {
            MyThread t=threads[i];
            t.closeChannel();
        }
        channel.close();
    }




    public static class MyThread extends Thread {
        int                index=-1;
        long                total_connect_time=0, total_disconnect_time=0;
        private JChannel    ch=null;
        private Address     my_addr=null;

        public MyThread(int i) {
            super("thread #" + i);
            index=i;
        }

        public void closeChannel() {
            if(ch != null) {
                ch.close();
            }
        }


        public void run() {
            View view;

            try {
                ch=new JChannel(props);

                start_connecting.barrier();

                long start=System.currentTimeMillis(), stop;
                ch.connect(groupname);
                stop=System.currentTimeMillis();
                total_connect_time=stop-start;
                view=ch.getView();
                my_addr=ch.getLocalAddress();
                log(my_addr + " connected in " + total_connect_time + " msecs (" +
                    view.getMembers().size() + " members). VID=" + view.getVid());

                connected.barrier();

                int num_members=0;
                while(true) {
                    View v=ch.getView();
                    Vector mbrs=v != null? v.getMembers() : null;
                    if(mbrs == null) {
                        System.err.println("mbrs is null, v=" + v);
                    }
                    else {
                        num_members=mbrs.size();
                        log("num_members=" + num_members);
                        if(num_members == NUM+1) // all threads (NUM) plus the first channel (1)
                            break;
                    }
                    Util.sleep(2000);
                }
                log("reached " + num_members + " members");
                received_all_views.barrier();

                start_disconnecting.barrier();
                start=System.currentTimeMillis();
                ch.disconnect();
                stop=System.currentTimeMillis();

                log(my_addr + " disconnected in " + (stop-start) + " msecs");
                disconnected.barrier();
            }
            catch(BrokenBarrierException e) {
                e.printStackTrace();
            }
            catch(ChannelException e) {
                e.printStackTrace();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }


    }


    public static Test suite() {
        TestSuite s=new TestSuite();
        // we're adding the tests manually, because they need to be run in *this exact order*
        s.addTest(new ConnectStressTest("testConcurrentJoins"));
        s.addTest(new ConnectStressTest("testConcurrentLeaves"));
        return s;
    }

    public static void main(String[] args) {
        String[] testCaseName={ConnectStressTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


}
