package org.jgroups.tests;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import junit.framework.TestCase;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Channel;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

/**
 * Tests concurrent startup
 * 
 * @author Brian Goose
 * @version $Id: ChannelConcurrencyTest.java,v 1.1.2.12 2008/06/05 03:15:41 vlada Exp $
 */
public class ChannelConcurrencyTest extends TestCase {

    public void test() throws Throwable {
        final int count=8;

        final Executor executor=Executors.newFixedThreadPool(count);
        final CountDownLatch latch=new CountDownLatch(count);
        final JChannel[] channels=new JChannel[count];
        final Task[] tasks=new Task[count];

        final long start=System.currentTimeMillis();
        for(int i=0;i < count;i++) {
            channels[i]=new JChannel("flush-udp.xml");
            tasks[i]=new Task(latch, channels[i]);
            changeMergeInterval(channels[i]);
            changeViewBundling(channels[i]);
        }

        for(final Task t:tasks) {
            executor.execute(t);
        }

        try {
            // Wait for all channels to finish connecting
            latch.await();

            for(Task t:tasks) {
                Throwable ex=t.getException();
                if(ex != null)
                    throw ex;
            }

            // Wait for all channels to have the correct number of members in their
            // current view
            boolean converged=false;
            for(int timeoutToConverge=120,counter=0;counter < timeoutToConverge && !converged;SECONDS.sleep(1),counter++) {
                for(final JChannel channel:channels) {
                    converged = channel.getView().size() == count;
                    if(!converged)
                        break;
                }                
            }

            final long duration=System.currentTimeMillis() - start;
            System.out.println("Converged to a single group after " + duration + " ms; group is:\n");
            for(int i=0;i < channels.length;i++) {
                System.out.println("#" + (i + 1) + ": " + channels[i].getLocalAddress() + ": " + channels[i].getView());
            }

            for(final JChannel channel:channels) {
                assertTrue("View ok for channel " + channel.getLocalAddress(),
                           count == channel.getView().size());
            }
        }
        finally {
            Util.sleep(2000);  
            List<Address> members = new ArrayList<Address>(channels[0].getView().getMembers());
            Collections.reverse(members);
            for(Address member:members){
            	for(final JChannel channel:channels) {            
                    if(member.equals(channel.getLocalAddress())){
                    	channel.close();
                    	Util.sleep(300);  
                    }
                }	
            }            

            for(final JChannel channel:channels) {
                assertFalse("Channel connected", channel.isConnected());
            }
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

    private static class Task implements Runnable {
        private final Channel c;
        private final CountDownLatch latch;
        private Throwable exception=null;


        public Task(CountDownLatch latch, Channel c) {
            this.latch=latch;
            this.c=c;
        }

        public Throwable getException() {
            return exception;
        }

        public void run() {
            try {
                c.connect("test");
            }
            catch(final Exception e) {
                exception=e;
                e.printStackTrace();
            }
            finally {
                latch.countDown();
            }
        }
    }
}