
package org.jgroups.tests;


import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.Global;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.VIEW_SYNC;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;


/**
 * Creates 1 channel, then creates NUM channels, all try to join the same channel concurrently.
 * @author Bela Ban Nov 20 2003
 * @version $Id: ConnectStressTest.java,v 1.38 2008/11/12 13:32:56 belaban Exp $
 */
@Test(groups=Global.STACK_DEPENDENT, sequential=false)
public class ConnectStressTest {
    static final int            NUM=20;
    private final CyclicBarrier barrier=new CyclicBarrier(NUM+1);
    private final MyThread[]    threads=new MyThread[NUM];
    static final String         groupname="ConcurrentTestDemo";
    static final String         props="udp.xml";




    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }


    public void testConcurrentJoinsAndLeaves() throws Exception {
        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread(i, barrier);
            threads[i].start();
        }

        barrier.await(); // causes all threads to call Channel.connect()

        // coordinator attempts to get complete view within 50 (5*10) seconds
        int min=NUM, max=0;
        for(int i=0; i < 20; i++) {
            for(MyThread thread: threads) {
                JChannel ch=thread.getChannel();
                View view=ch.getView();
                if(view != null) {
                    int size=view.size();
                    min=Math.min(size, NUM);
                    max=Math.max(size, max);
                }
            }

            if(min >= NUM && max >= NUM)
                break;
            System.out.println("min=" + min + ", max=" + max);
            Util.sleep(2000);
        }

        System.out.println("reached " + NUM + " members: min=" + min + ", max=" + max);
        assert min >= NUM && max >= NUM : "min=" + min + ", max=" + max + ", expected: " + NUM;

        System.out.println("Starting the disconnect phase");

        for(int i=0; i < threads.length; i++) {
            MyThread thread=threads[i];
            System.out.print("disconnecting " + thread.getName());
            thread.disconnect();
            System.out.println(" OK");
        }
    }




    public static class MyThread extends Thread {
        private final CyclicBarrier barrier;
        private JChannel ch=null;

        public MyThread(int i, CyclicBarrier barrier) {
            super("thread #" + i);
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
                ch=new JChannel(props);
                changeProperties(ch);
                barrier.await(); // wait for all threads to be running
                ch.connect(groupname);
            }
            catch(Exception e) {
            }
        }
    }

    private static void changeProperties(JChannel ch) {
        ch.setOpt(Channel.AUTO_RECONNECT, true);
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
        VIEW_SYNC sync=(VIEW_SYNC)stack.findProtocol(VIEW_SYNC.class);
        if(sync != null)
            sync.setAverageSendInterval(5000);
        NAKACK nakack=(NAKACK)stack.findProtocol(NAKACK.class);
        if(nakack != null)
            nakack.setLogDiscardMsgs(false);
    }




}
