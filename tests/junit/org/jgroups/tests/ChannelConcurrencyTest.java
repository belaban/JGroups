package org.jgroups.tests;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.jgroups.JChannel;
import org.jgroups.Channel;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;

/**
 * Tests concurrent startup
 * 
 * @author Brian Goose
 * @version $Id: ChannelConcurrencyTest.java,v 1.1.2.5 2008/06/03 10:59:18 belaban Exp $
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
            changeMergeInterval(channels[i]);
            changeViewBundling(channels[i]);
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
                SECONDS.sleep(1);
            }
        }

        final long duration=System.currentTimeMillis() - start;
        System.out.println("Converged to a single group after " + duration + " ms; group is:\n");
        for(int i=0; i < channels.length; i++) {
            System.out.println("#" + (i+1) + ": " + channels[i].getLocalAddress() + ": " + channels[i].getView());
        }
    }


    private static void changeViewBundling(JChannel channel) {
        ProtocolStack stack=channel.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        if(gms != null) {
            gms.setViewBundling(true);
            gms.setMaxBundlingTime(500);
        }
    }

    private static void changeMergeInterval(JChannel channel) {
        ProtocolStack stack=channel.getProtocolStack();
        MERGE2 merge=(MERGE2)stack.findProtocol(MERGE2.class);
        if(merge != null) {
            merge.setMinInterval(5000);
            merge.setMaxInterval(10000);
        }
    }
}