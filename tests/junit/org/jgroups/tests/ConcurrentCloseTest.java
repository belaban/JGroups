package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.CyclicBarrier;

/**
 * @author Bela Ban
 * @version $Id: ConcurrentCloseTest.java,v 1.4 2008/04/09 15:01:36 belaban Exp $
 */
public class ConcurrentCloseTest extends ChannelTestBase {
    JChannel c1, c2;


    @AfterMethod
    public void tearDown() throws Exception {
        if(c2 != null) {
            c2.close();
            c2=null;
        }

        if(c1 != null) {
            c1.close();
            c1=null;
        }

        ;
    }

    /**
     * 2 channels, both call Channel.close() at exactly the same time
     */
    @Test
    public void testConcurrentClose() throws Exception {
        System.setProperty("useBlocking", "true"); // enables reception of block() and unblock() callbacks
        c1=createChannel();
        c1.setReceiver(new MyReceiver("C1"));
        c2=createChannel();
        c2.setReceiver(new MyReceiver("C2"));
        c1.connect("x");
        c2.connect("x");
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
        private final Channel channel;
        final private CyclicBarrier barrier;


        public Closer(Channel channel, CyclicBarrier barrier) {
            this.channel=channel;
            this.barrier=barrier;
        }

        public void run() {
            try {
                barrier.await();
                System.out.println("closing channel for " + channel.getLocalAddress());
                channel.close();
            }
            catch(Exception e) {
            }
        }
    }

    private static class MyReceiver extends ExtendedReceiverAdapter {
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
