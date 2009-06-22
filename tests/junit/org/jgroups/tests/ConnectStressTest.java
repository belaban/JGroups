
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;


/**
 * Creates 1 channel, then creates NUM channels, all try to join the same channel concurrently.
 * @author Bela Ban Nov 20 2003
 * @version $Id: ConnectStressTest.java,v 1.43 2009/06/22 11:46:31 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT, sequential=false)
public class ConnectStressTest extends ChannelTestBase {
    static final int            NUM=20;
    private final CyclicBarrier barrier=new CyclicBarrier(NUM+1);
    private final MyThread[]    threads=new MyThread[NUM];
    static final String         groupname="ConcurrentStressTest";




    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }


    public void testConcurrentJoinsAndLeaves() throws Exception {
        JChannel first=null;
        for(int i=0; i < threads.length; i++) {
            JChannel ch=null;
            if(i == 0) {
                ch=createChannel(true, NUM);
                first=ch;
            }
            else
                ch=createChannel(first);
            changeProperties(ch);
            threads[i]=new MyThread(ch, i+1, barrier);
            threads[i].start();
        }

        barrier.await(); // causes all threads to call Channel.connect()
        System.out.println("*** Starting the connect phase ***");

        long target_time=System.currentTimeMillis() + 30000L;
        while(System.currentTimeMillis() < target_time) {
            View view=threads[0].getChannel().getView();
            if(view != null) {
                int size=view.size();
                if(size >= NUM)
                    break;
            }
            Util.sleep(1000L);
        }

        for(MyThread thread: threads) {
            View view=thread.getChannel().getView();
            System.out.println("#" + thread.getName() + ": " + (view != null? view.getViewId() + ", size=" + view.size() : view));
        }

        for(MyThread thread: threads) {
            View view=thread.getChannel().getView();
            int size=view.size();
            assert size == NUM : "view doesn't have size of " + NUM + " (has " + size + "): " + view;
        }


        System.out.println("*** Starting the disconnect phase ***");

        for(int i=0; i < threads.length; i++) {
            MyThread thread=threads[i];
            System.out.print("disconnecting " + thread.getName());
            thread.disconnect();
            System.out.println(" OK");
        }
    }




    public static class MyThread extends Thread {
        private final CyclicBarrier barrier;
        private final JChannel ch;

        public MyThread(JChannel channel, int i, CyclicBarrier barrier) {
            super("thread #" + i);
            this.ch=channel;
            this.barrier=barrier;
        }

        public void disconnect() {
            ch.disconnect();
        }

        public void close() {
            Util.close(ch);
        }

        public JChannel getChannel() {
            return ch;
        }

        public void run() {
            try {
                barrier.await(); // wait for all threads to be running
                ch.connect(groupname);
            }
            catch(Exception e) {
            }
        }
    }

    private static void changeProperties(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol("GMS");
        if(gms != null) {
            gms.setViewBundling(true);
            gms.setMaxBundlingTime(300);
            gms.setPrintLocalAddr(false);
        }
        MERGE2 merge=(MERGE2)stack.findProtocol("MERGE2");
        if(merge != null) {
            merge.setMinInterval(2000);
            merge.setMaxInterval(5000);
        }
        NAKACK nakack=(NAKACK)stack.findProtocol(NAKACK.class);
        if(nakack != null)
            nakack.setLogDiscardMsgs(false);
    }




}
