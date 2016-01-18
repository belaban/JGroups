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
    JChannel c1, c2;


    @AfterMethod
    void tearDown() throws Exception {
        Util.close(c2, c1);
    }

    /**
     * 2 channels, both call Channel.close() at exactly the same time
     */
    public void testConcurrentClose() throws Exception {
        System.setProperty("useBlocking", "true"); // enables reception of block() and unblock() callbacks
        c1=createChannel(true);
        c1.setReceiver(new MyReceiver("C1"));

        c2=createChannel(c1);
        c2.setReceiver(new MyReceiver("C2"));

        final String GROUP=getUniqueClusterName("ConcurrentCloseTest");
        c1.connect(GROUP);
        c2.connect(GROUP);
        CyclicBarrier barrier=new CyclicBarrier(3);

        Closer one=new Closer(c1, barrier), two=new Closer(c2, barrier);
        one.start(); two.start();
        Util.sleep(500);
        barrier.await(); // starts the closing of the 2 channels
        one.join(10000);
        two.join(10000);
        assertFalse(one.isAlive());
        assertFalse(two.isAlive());
    }


    private static class Closer extends Thread {
        private final AbstractChannel channel;
        final private CyclicBarrier barrier;


        public Closer(AbstractChannel channel, CyclicBarrier barrier) {
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

    private static class MyReceiver extends ReceiverAdapter {
        private final String name;

        public MyReceiver(String name) {
            this.name=name;
        }

        public void block() {
            System.out.println("[" + name + "] block()");
        }

        public void unblock() {
            System.out.println("[" + name + "] unblock()");
        }

        public void viewAccepted(View new_view) {
            System.out.println("[" + name + "] " + new_view);
        }

        public void receive(Message msg) {
            System.out.println("[" + name + "] " + msg);
        }
    }

}
