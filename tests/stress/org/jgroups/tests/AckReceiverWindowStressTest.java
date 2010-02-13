package org.jgroups.tests;

import org.jgroups.stack.AckReceiverWindow;

import java.util.concurrent.CountDownLatch;

/**
 * Tests time for N threads to insert and remove M messages into an AckReceiverWindow
 * @author Bela Ban
 * @version $Id: AckReceiverWindowStressTest.java,v 1.1 2010/02/13 13:22:11 belaban Exp $
 */
public class AckReceiverWindowStressTest {

    void start(int num_threads, int num_msgs) {
        final AckReceiverWindow win=new AckReceiverWindow(1);
        
    }


    static class Adder extends Thread {
        final AckReceiverWindow win;
        final CountDownLatch latch;
        final int num_msgs;

        public Adder(AckReceiverWindow win, CountDownLatch latch, int num_msgs) {
            this.win=win;
            this.latch=latch;
            this.num_msgs=num_msgs;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return;
            }

            for(int i=0; i < num_msgs; i++) {
                
            }
        }
    }


    public static void main(String[] args) {
        int num_threads=100;
        int num_msgs=10000;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num_threads")) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_msgs")) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("AckReceiverWindowStressTest [-num_msgs msgs] [-num_threads threads]");
            return;
        }
        AckReceiverWindowStressTest test=new AckReceiverWindowStressTest();
        test.start(num_threads, num_msgs);
    }
}
