// $Id: MergeStressTest.java,v 1.8 2008/04/08 07:18:57 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.Assert;

import java.util.concurrent.CyclicBarrier;


/**
 * Creates NUM channels, all trying to join the same channel concurrently. This will lead to singleton groups
 * and subsequent merging. To enable merging, GMS.handle_concurrent_startup has to be set to false.
 * @author Bela Ban
 * @version $Id: MergeStressTest.java,v 1.8 2008/04/08 07:18:57 belaban Exp $
 */
public class MergeStressTest {
    static CyclicBarrier start_connecting=null;
    static CyclicBarrier    received_all_views=null;
    static CyclicBarrier    start_disconnecting=null;
    static CyclicBarrier    disconnected=null;
    static final int        NUM=10;
    static final long       TIMEOUT=50000;
    static final MyThread[] threads=new MyThread[NUM];
    static String           groupname="ConcurrentTestDemo";


    static String props="UDP(mcast_addr=228.8.8.9;mcast_port=7788;ip_ttl=1;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000):" +
            "PING(timeout=3000;num_initial_members=3):" +
            "MERGE2(min_interval=3000;max_interval=5000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=300,600,1200,2400,4800):" +
            "UNICAST(timeout=300,600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=5000):" +
            "FRAG(frag_size=4096):" +
            "pbcast.GMS(join_timeout=5000;" +
            "shun=false;print_local_addr=false;view_ack_collection_timeout=5000;" +
            "merge_timeout=30000;handle_concurrent_startup=false)";



    public MergeStressTest(String name) {
    }


    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }


    @org.testng.annotations.Test
    public void testConcurrentStartupAndMerging() throws Exception {
        start_connecting=new CyclicBarrier(NUM+1);
        received_all_views=new CyclicBarrier(NUM+1);
        start_disconnecting=new CyclicBarrier(NUM+1);
        disconnected=new CyclicBarrier(NUM+1);

        long start, stop;

        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(i);
            threads[i].start();
        }

        // signal the threads to start connecting to their channels
        Util.sleep(1000);
        start_connecting.await();
        start=System.currentTimeMillis();

        try {
            received_all_views.await();
            stop=System.currentTimeMillis();
            System.out.println("-- took " + (stop-start) + " msecs for all " + NUM + " threads to see all views");

            int num_members;
            MyThread t;
            System.out.print("checking that all views have " + NUM + " members: ");
            for(int i=0; i < threads.length; i++) {
                t=threads[i];
                num_members=t.numMembers();
                Assert.assertEquals(num_members, NUM);
            }
            System.out.println("SUCCESSFUL");
        }
        catch(Exception ex) {
            assert false : ex.toString();
        }
        finally {
            start_disconnecting.await();
            disconnected.await();
        }
    }





    public static class MyThread extends ReceiverAdapter implements Runnable {
        int                 index=-1;
        long                total_connect_time=0, total_disconnect_time=0;
        private JChannel    ch=null;
        private Address     my_addr=null;
        private View        current_view;
        private Thread      thread;
        private int         num_members=0;



        public MyThread(int i) {
            thread=new Thread(this, "thread #" + i);
            index=i;
        }

        public void start() {
            thread.start();
        }

        public void closeChannel() {
            if(ch != null) {
                ch.close();
            }
        }

        public int numMembers() {
            return ch.getView().size();
        }


        public void viewAccepted(View new_view) {
            String type="view";
            if(new_view instanceof MergeView)
                type="merge view";
            if(current_view == null) {
                current_view=new_view;
                log(type + " accepted: " + current_view.getVid() + " :: " + current_view.getMembers());
            }
            else {
                if(!current_view.equals(new_view)) {
                    current_view=new_view;
                    log(type + " accepted: " + current_view.getVid() + " :: " + current_view.getMembers());
                }
            }

            num_members=current_view.getMembers().size();
            if(num_members == NUM) {
                synchronized(this) {
                    this.notifyAll();
                }
            }
        }


        public void run() {
            View view;

            try {
                start_connecting.await();
                ch=new JChannel(props);
                ch.setReceiver(this);
                log("connecting to channel");
                long start=System.currentTimeMillis(), stop;
                ch.connect(groupname);
                stop=System.currentTimeMillis();
                total_connect_time=stop-start;
                view=ch.getView();
                my_addr=ch.getLocalAddress();
                log(my_addr + " connected in " + total_connect_time + " msecs (" +
                    view.getMembers().size() + " members). VID=" + ch.getView());

                synchronized(this) {
                    while(num_members < NUM) {
                        try {this.wait();} catch(InterruptedException e) {}
                    }
                }

                log("reached " + num_members + " members");
                received_all_views.await();

                start_disconnecting.await();
                start=System.currentTimeMillis();
                ch.shutdown();
                stop=System.currentTimeMillis();

                log(my_addr + " shut down in " + (stop-start) + " msecs");
                disconnected.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }


    }


    public static Test suite() {
        return new TestSuite(MergeStressTest.class);
    }

    public static void main(String[] args) {
        String[] testCaseName={MergeStressTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }


}
