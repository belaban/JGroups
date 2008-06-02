package org.jgroups.tests;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.jgroups.JChannel;
import org.jgroups.Channel;

/**
 * Tests concurrent startup
 * 
 * @author Brian Goose
 * @version $Id: ChannelConcurrencyTest.java,v 1.1.2.1 2008/05/31 05:03:38
 *          belaban Exp $
 */
public class ChannelConcurrencyTest extends TestCase {

    public void test() throws Exception {
        final int count=8;

        final Executor executor=Executors.newFixedThreadPool(count);
        final CountDownLatch latch=new CountDownLatch(count);
        final JChannel[] channels=new JChannel[count];

        final long start=System.currentTimeMillis();
        for(int i=0;i < count;i++) {
            channels[i]=new JChannel("flush-udp.xml");
        }

        for(final Channel c:channels) {
            executor.execute(new Runnable() {

                public void run() {
                    try {
                        c.connect("test");
                        latch.countDown();
                    }
                    catch(final Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        // Wait for all channels to finish connecting
        latch.await();

        // Wait for all channels to have the correct number of members in their
        // current view
        for(;;) {
            boolean done=true;
            for(final JChannel channel:channels) {
                if(channel.getView().size() < count) {
                    done=false;
                }
            }
            if(done) {
                break;
            }
            else {
                SECONDS.sleep(100);
            }
        }

        for(final Channel ch:channels) {
            System.out.println(ch.getView());
        }

        final long duration=System.currentTimeMillis() - start;
        System.out.println("Converged to a single group after " + duration + " ms");
    }
}