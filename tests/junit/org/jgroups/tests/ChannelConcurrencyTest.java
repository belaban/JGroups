package org.jgroups.tests;

import static java.util.concurrent.TimeUnit.SECONDS;

import java.net.InetAddress;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.GroupRequest;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.MPING;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests concurrent startup
 * @author Brian Goose
 * @version $Id: ChannelConcurrencyTest.java,v 1.15 2008/07/24 17:36:26 vlada Exp $
 */
@Test(groups=Global.FLUSH,sequential=true)
public class ChannelConcurrencyTest  extends ChannelTestBase{

    public void testPlainChannel () throws Throwable{
        testhelper(false);
    }
    
    public void testwithDispatcher () throws Throwable{
        testhelper(true);
    }
    
    protected  void testhelper(boolean useDispatcher) throws Throwable {
        final int count=8;

        final Executor executor=Executors.newFixedThreadPool(count);
        final CountDownLatch latch=new CountDownLatch(count);
        final JChannel[] channels=new JChannel[count];
        final Task[] tasks=new Task[count];

        final long start=System.currentTimeMillis();
        for(int i=0;i < count;i++) {
            if(i == 0)
                channels[i]=createChannel(true, count);
            else
                channels[i]=createChannel(channels[0]);
            
            tasks[i]=new Task(latch, channels[i],useDispatcher);
            changeMergeInterval(channels[i]);
            changeViewBundling(channels[i]);
            replaceDiscoveryProtocol(channels[i]);
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
                    converged = channel.getView() != null && channel.getView().size() == count;
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
                AssertJUnit.assertSame("View ok for channel " + channel.getLocalAddress(), count, channel.getView().size());
            }
        }
        finally {
            System.out.print("closing channels: ");
            for(int i=channels.length -1; i>= 0; i--) {
                Channel channel=channels[i];
                channel.close();
                
                //there are sometimes big delays until entire cluster shuts down
                //use sleep to make a smoother shutdown so we avoid false positives
                Util.sleep(300);
                int tries=0;
                while((channel.isConnected() || channel.isOpen()) && tries++ < 10) {
                    Util.sleep(1000);
                }
            }
            System.out.println("OK");

            for(final JChannel channel: channels) {
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
    
    private static void replaceDiscoveryProtocol(JChannel ch) throws Exception {
        ProtocolStack stack=ch.getProtocolStack();
        Protocol discovery=stack.removeProtocol("TCPPING");
        if(discovery != null){
            Protocol transport = stack.getTransport();
            MPING mping=new MPING();
            InetAddress bindAddress=Util.getBindAddress(new Properties());
            mping.setBindAddr(bindAddress);
            mping.setMulticastAddress("230.1.2.3");
            mping.setMcastPort(8888);            
            stack.insertProtocol(mping, ProtocolStack.ABOVE, transport.getName());
            mping.setProtocolStack(ch.getProtocolStack());
            mping.init();
            mping.start();            
            System.out.println("Replaced TCPPING with MPING. See http://wiki.jboss.org/wiki/Wiki.jsp?page=JGroupsMERGE2");            
        }        
    }

    private static class Task implements Runnable {
        private final Channel c;
        private final CountDownLatch latch;
        private Throwable exception=null;
        private boolean useDispatcher = false;


        public Task(CountDownLatch latch, Channel c,boolean useDispatcher) {
            this.latch=latch;
            this.c=c;
            this.useDispatcher = useDispatcher;
        }

        public Throwable getException() {
            return exception;
        }

        public void run() {
            try {
                c.connect("ChannelConcurrencyTest");
                if(useDispatcher) {
                    final MessageDispatcher md=new MessageDispatcher(c, null, null, new MyHandler());
                    for(int i=0;i < 10;i++) {
                        final RspList rsp=md.castMessage(null,
                                                         new Message(null, null, i),
                                                         GroupRequest.GET_ALL,
                                                         2500);
                        for(Object o:rsp.getResults()) {
                            assertEquals("Wrong result received at " + c.getLocalAddress(), i, o);
                        }
                    }
                }
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
    private static class MyHandler implements RequestHandler {

        public Object handle(Message msg) {
            return msg.getObject();
        }

    }
}