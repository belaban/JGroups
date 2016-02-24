
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.FD_SOCK;
import org.jgroups.protocols.MERGE3;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Creates NUM channels, all trying to join the same channel concurrently. This
 * will lead to singleton groups and subsequent merging. To enable merging,
 * GMS.handle_concurrent_startup has to be set to false.
 * 
 * @author Bela Ban
 */


/*
 * Occasionally fails under tcp stack on Cruise Control. Needs investigation
 * Vladimir Sept 22, 2008
 * 
 * */
@Test(groups=Global.STACK_DEPENDENT)
public class MergeStressTest extends ChannelTestBase {
    CyclicBarrier start_connecting=null;
    CyclicBarrier received_all_views=null;
    CyclicBarrier disconnected=null;
    static final int NUM=10;
    final MyThread[] threads=new MyThread[NUM];
    static String groupname="MergeStressTest";

    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }

    public void testConcurrentStartupAndMerging() throws Exception {
        start_connecting=new CyclicBarrier(NUM + 1);
        received_all_views=new CyclicBarrier(NUM + 1);
        disconnected=new CyclicBarrier(NUM + 1);

        long start, stop;
        JChannel first=null;

        for(int i=0;i < threads.length;i++) {
            JChannel tmp;
            if(i == 0) {
                first=createChannel(true, threads.length);
                tmp=first;
            }
            else {
                tmp=createChannel(first);
            }
            tmp.setName(String.valueOf(i+1));
            modifyStack(tmp);
            threads[i]=new MyThread(i, tmp);
            threads[i].start();
        }

        // signal the threads to start connecting to their channels
        Util.sleep(1000);
        start_connecting.await();
        start=System.currentTimeMillis();

        try {
            received_all_views.await(45, TimeUnit.SECONDS);
            stop=System.currentTimeMillis();
            System.out.println("-- took " + (stop - start)
                               + " msecs for all "
                               + NUM
                               + " threads to see all views");

            int num_members;
            MyThread t;
            System.out.print("checking that all views have " + NUM + " members: ");
            for(int i=0;i < threads.length;i++) {
                t=threads[i];
                num_members=t.numMembers();
                Assert.assertEquals(num_members, NUM);
            }
            System.out.println("SUCCESSFUL");
        }
        catch(TimeoutException timeoutOnReceiveViews) {
            for(MyThread channel:threads) {
                channel.interrupt();
            }
        }
        catch(Exception ex) {
            assert false:ex.toString();
        }
        finally {
            disconnected.await();
        }
    }

    private static void modifyStack(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol prot=stack.findProtocol(MERGE3.class);
        if(prot != null) {
            MERGE3 merge=(MERGE3)prot;
            merge.setMinInterval(3000);
            merge.setMaxInterval(5000);
        }
        prot=stack.findProtocol(STABLE.class);
        if(prot != null) {
            STABLE stable=(STABLE)prot;
            stable.setDesiredAverageGossip(5000);
        }
        prot=stack.findProtocol(NAKACK2.class);
        if(prot != null) {
            ((NAKACK2)prot).setLogDiscardMessages(false);
        }
        prot=stack.findProtocol(FD_SOCK.class);
        if(prot != null) {
            ((FD_SOCK)prot).setLogSuspectedMessages(false);
        }
    }

    public class MyThread extends ReceiverAdapter implements Runnable {
        int index=-1;
        long total_connect_time=0, total_disconnect_time=0;
        private final JChannel ch;
        private Address my_addr=null;
        private View current_view;
        private final Thread thread;
        private int num_members=0;

        public MyThread(int i,JChannel ch) {
            this.ch=ch;
            thread=new Thread(this, "thread #" + i);
            index=i;
        }

        public void start() {
            thread.start();
        }

        public void interrupt() {
            thread.interrupt();
        }

        public void closeChannel() {
            Util.close(ch);
        }

        public int numMembers() {
            return ch.getView().size();
        }

        public void viewAccepted(View new_view) {
            current_view=new_view;
            log("accepted " + new_view);
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
                ch.setReceiver(this);
                log("connecting to channel");
                long start=System.currentTimeMillis(), stop;
                ch.connect(groupname);
                stop=System.currentTimeMillis();
                total_connect_time=stop - start;
                view=ch.getView();
                my_addr=ch.getAddress();
                log(my_addr + " connected in "
                    + total_connect_time
                    + " msecs ("
                    + view.getMembers().size()
                    + " members). VID="
                    + ch.getView());

                synchronized(this) {
                    while(num_members < NUM && !Thread.currentThread().isInterrupted()) {
                        try {
                            this.wait();
                        }
                        catch(InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            finally {
                log("reached " + num_members + " members");
                try {
                    received_all_views.await();
                    Util.shutdown(ch);
                    disconnected.await();
                }
                catch(Exception e) {
                }
            }
        }
    }
}
