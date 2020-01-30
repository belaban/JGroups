package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class ConcurrentCloseTest extends ChannelTestBase {
    JChannel a, b;


    @AfterMethod void tearDown() throws Exception {Util.close(b, a);}

    /** 2 channels, both call Channel.close() at exactly the same time */
    public void testConcurrentClose() throws Exception {
        final String GROUP="ConcurrentCloseTest";
        a=createChannel(true).name("A").setReceiver(new MyReceiver("A")).connect(GROUP);
        b=createChannel(a).name("B").setReceiver(new MyReceiver("B")).connect(GROUP);
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, a,b);

        CyclicBarrier barrier=new CyclicBarrier(3);
        Closer one=new Closer(a, barrier), two=new Closer(b, barrier);
        one.start(); two.start();
        Util.sleep(500);
        barrier.await(); // starts the closing of the 2 channels
        one.join(10000);
        two.join(10000);
        assertFalse(one.isAlive());
        assertFalse(two.isAlive());
    }


    private static class Closer extends Thread {
        private final JChannel channel;
        final private CyclicBarrier barrier;


        public Closer(JChannel channel, CyclicBarrier barrier) {
            this.channel=channel;
            this.barrier=barrier;
        }

        public void run() {
            try {
                barrier.await();
                System.out.println("closing channel for " + channel.getAddress());
                channel.close();
            }
            catch(Exception e) {
            }
        }
    }

    private static class MyReceiver implements Receiver {
        private final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public void viewAccepted(View new_view) {
            System.out.println("[" + name + "] " + new_view);
        }

        public void receive(Message msg) {
            System.out.println("[" + name + "] " + msg);
        }
    }

}
