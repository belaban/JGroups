
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;


/**
 * Creates 1 channel, then creates NUM channels, all try to join the same channel concurrently.
 * @author Bela Ban Nov 20 2003
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class ConnectStressTest {
    protected static final int    NUM=20;
    protected final CyclicBarrier barrier=new CyclicBarrier(NUM+1);
    protected final JChannel[]    channels=new JChannel[NUM];
    protected final MyThread[]    threads=new MyThread[NUM];



    static void log(String msg) {
        System.out.println("-- [" + Thread.currentThread().getName() + "] " + msg);
    }

    @BeforeMethod protected void setup() throws Exception {
        for(int i=0; i < NUM; i++) {
            channels[i]=createChannel(String.valueOf((char)(i + 1)));
            if(i == 0)
                channels[i].connect("ConnectStressTest");
        }
    }

    @AfterMethod protected void destroy() {Util.closeFast(channels);}


    public void testConcurrentJoining() throws Exception {
        for(int i=0; i < NUM; i++) {
            threads[i]=new MyThread(channels[i], i+1, barrier);
            threads[i].start();
        }

        barrier.await(); // causes all threads to call Channel.connect()
        System.out.println("*** Starting the connect phase ***");

        long target_time=System.currentTimeMillis() + 60000L;
        while(System.currentTimeMillis() < target_time) {
            View view=channels[0].getView();
            if(view != null) {
                int size=view.size();
                System.out.println("channel[0].view has " + size + " members (expected: " + NUM + ")");
                if(size >= NUM)
                    break;
            }
            Util.sleep(1000L);
        }

        for(JChannel ch: channels) {
            View view=ch.getView();
            if(view != null)
                System.out.println(ch.getName() + ": size=" + view.size() + ", view-id: " + view.getViewId());
        }

        for(JChannel ch: channels) {
            View view=ch.getView();
            int size=view != null? view.size() : 0;
            assert view != null && size == NUM : "view doesn't have size of " + NUM + " (has " + size + "): " + view;
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

        public void run() {
            try {
                barrier.await(); // wait for all threads to be running
                ch.connect("ConnectStressTest");
            }
            catch(Exception e) {
            }
        }
    }

    protected static JChannel createChannel(String name) throws Exception {
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            new MERGE3().setMinInterval(2000).setMaxInterval(5000),
                            new NAKACK2().logDiscardMessages(false),
                            new UNICAST3(),
                            new STABLE(),
                            new GMS().setJoinTimeout(1000).printLocalAddress(false),
                            new FRAG2().setFragSize(8000));

    }



}
