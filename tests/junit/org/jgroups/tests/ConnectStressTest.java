
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;


/**
 * Creates 1 channel, then creates NUM channels, all try to join the same channel concurrently.
 * @author Bela Ban Nov 20 2003
 */
@Test(groups={Global.STACK_INDEPENDENT}, sequential=true)
public class ConnectStressTest extends ChannelTestBase {
    static final int            NUM=20;
    private final CyclicBarrier barrier=new CyclicBarrier(NUM+1);
    private final MyThread[]    threads=new MyThread[NUM];
    static final String         groupname="ConnectStressTest";



    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }

    @AfterMethod
    void destroy() {
        for(int i=0; i < threads.length; i++) {
            MyThread thread=threads[i];
            JChannel ch=thread.getChannel();
            ProtocolStack stack=ch.getProtocolStack();
            stack.stopStack(ch.getClusterName());
            stack.destroy();
        }
    }


    public void testConcurrentJoining() throws Exception {
        JChannel first=null;
        channel_conf="udp.xml";
        
        for(int i=0; i < threads.length; i++) {
            JChannel ch=null;
            if(i == 0) {
                ch=createChannel(true, NUM);
                first=ch;
            }
            else
                ch=createChannel(first);
            ch.setName(String.valueOf(i + 1));
            changeProperties(ch);
            threads[i]=new MyThread(ch, i+1, barrier);
            threads[i].start();
        }

        barrier.await(); // causes all threads to call Channel.connect()
        System.out.println("*** Starting the connect phase ***");

        long target_time=System.currentTimeMillis() + 60000L;
        while(System.currentTimeMillis() < target_time) {
            View view=threads[0].getChannel().getView();
            if(view != null) {
                int size=view.size();
                System.out.println("channel[0].view has " + size + " members (expected: " + NUM + ")");
                if(size >= NUM)
                    break;
            }
            Util.sleep(2000L);
        }

        for(MyThread thread: threads) {
            View view=thread.getChannel().getView();
            if(view != null)
                System.out.println(thread.getName() + ": size=" + view.size() + ", view-id: " + view.getViewId());
        }

        for(MyThread thread: threads) {
            View view=thread.getChannel().getView();
            int size=view.size();
            assert size == NUM : "view doesn't have size of " + NUM + " (has " + size + "): " + view;
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
        NAKACK2 nakack=(NAKACK2)stack.findProtocol(NAKACK2.class);
        if(nakack != null)
            nakack.setLogDiscardMessages(false);
    }




}
