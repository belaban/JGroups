// $Id: MergeStressTest.java,v 1.12 2008/05/23 10:45:43 belaban Exp $

package org.jgroups.tests;



import org.testng.annotations.*;
import org.jgroups.*;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.Assert;

import java.util.concurrent.CyclicBarrier;
import java.util.Properties;


/**
 * Creates NUM channels, all trying to join the same channel concurrently. This will lead to singleton groups
 * and subsequent merging. To enable merging, GMS.handle_concurrent_startup has to be set to false.
 * @author Bela Ban
 * @version $Id: MergeStressTest.java,v 1.12 2008/05/23 10:45:43 belaban Exp $
 */
public class MergeStressTest extends ChannelTestBase {
    CyclicBarrier           start_connecting=null;
    CyclicBarrier           received_all_views=null;
    CyclicBarrier           start_disconnecting=null;
    CyclicBarrier           disconnected=null;
    static final int        NUM=10;
    static final long       TIMEOUT=50000;
    final MyThread[]        threads=new MyThread[NUM];
    static String           groupname="MergeStressTest";



    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }


    @Test
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





    public class MyThread extends ReceiverAdapter implements Runnable {
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
                ch=createChannel();
                modifyStack(ch);
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


        private void modifyStack(JChannel ch) {
            ProtocolStack stack=ch.getProtocolStack();
            Properties props=new Properties();
            Protocol prot=stack.findProtocol(MERGE2.class);
            if(prot != null) {
                MERGE2 merge=(MERGE2)prot;
                merge.setMinInterval(3000);
                merge.setMaxInterval(5000);
            }
            prot=stack.findProtocol(STABLE.class);
            if(prot != null) {
                STABLE stable=(STABLE)prot;
                stable.setDesiredAverageGossip(5000);
            }
        }


    }



}
