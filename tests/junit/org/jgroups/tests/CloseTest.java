// $Id: CloseTest.java,v 1.10 2006/09/17 07:56:22 belaban Exp $

package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.util.Util;

import java.util.Vector;


/**
 * Demos the creation of a channel and subsequent connection and closing. Demo application should exit (no
 * more threads running)
 */
public class CloseTest extends TestCase {
    JChannel channel, channel1, channel2, c1, c2, c3;
    int active_threads=0;

    String props="UDP(mcast_addr=228.8.8.3;mcast_port=45577;ip_ttl=32;" +
            "mcast_send_buf_size=150000;mcast_recv_buf_size=80000;" +
            "enable_bundling=true;max_bundle_timeout=30;use_incoming_packet_handler=true;loopback=true):" +
            "PING(timeout=2000;num_initial_members=3):" +
            "MERGE2(min_interval=5000;max_interval=10000):" +
            "FD_SOCK:" +
            "VERIFY_SUSPECT(timeout=1500):" +
            "pbcast.NAKACK(gc_lag=50;retransmit_timeout=600,1200,2400,4800):" +
            "UNICAST(timeout=600,1200,2400):" +
            "pbcast.STABLE(desired_avg_gossip=20000):" +
            "FRAG(frag_size=4096;down_thread=false;up_thread=false):" +
            "pbcast.GMS(join_timeout=5000;join_retry_timeout=2000;" +
            "shun=true;print_local_addr=true)";

    public CloseTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        String cfg=System.getProperty("config");
        if(cfg != null)
            props=cfg;
        active_threads=Thread.activeCount();
        System.out.println("active threads before (" + active_threads + "):\n" + Util.activeThreads());
    }


    protected void tearDown() throws Exception {
        super.tearDown();
        closeChannel(channel);
        closeChannel(channel1);
        closeChannel(channel2);
        closeChannel(c1);
        closeChannel(c2);
        closeChannel(c3);

        int current_active_threads=Thread.activeCount();
        System.out.println("active threads after (" + current_active_threads + "):\n" + Util.activeThreads());
        // System.out.println("thread:\n" + dumpThreads());
        assertEquals(active_threads, current_active_threads);
    }

    private void closeChannel(JChannel c) {
        if(c != null && (c.isOpen() || c.isConnected())) {
            c.close();
        }
    }


    public void testDoubleClose() throws ChannelException {
        System.out.println("-- creating channel1 --");
        channel1=new JChannel(props);
        System.out.println("-- connecting channel1 --");
        channel1.connect("bla");
        System.out.println("-- closing channel1 --");
        channel1.close();
        System.out.println("-- closing channel1 (again) --");
        channel1.close();
        System.out.println("-- done, threads are ");
        Util.printThreads();
    }

    public void testCreationAndClose() throws Exception {
        System.out.println("-- creating channel1 --");
        channel1=new JChannel(props);
        System.out.println("-- connecting channel1 --");
        channel1.connect("CloseTest1");
        System.out.println("-- closing channel1 --");
        channel1.close();
        System.out.println("-- done, threads are ");
        Util.printThreads();
    }

    public void testViewChangeReceptionOnChannelCloseByParticipant() throws Exception {
        Address  a1, a2;
        Vector members;

        c1=new JChannel(props);
        System.out.println("-- connecting c1");
        c1.connect("X");
        Util.sleep(500); // time to receive its own view
        dumpMessages("c1", c1);
        a1=c1.getLocalAddress();

        c2=new JChannel(props);
        System.out.println("-- connecting c2");
        c2.connect("X");
        Util.sleep(500); // time to receive its own view
        a2=c2.getLocalAddress();
        dumpMessages("c2", c2);

        System.out.println("-- closing c2");
        c2.close();
        Object obj=c1.receive(100);
        assertTrue(obj instanceof View);
        View v=(View)obj;
        members=v.getMembers();
        System.out.println("-- first view of c1: " + v);
        assertEquals(2, members.size());
        assertTrue(members.contains(a1));
        assertTrue(members.contains(a2));

        obj=c1.receive(100);
        assertTrue(obj instanceof View);
        v=(View)obj;
        members=v.getMembers();
        System.out.println("-- second view of c1: " + v);
        assertEquals(1, members.size());
        assertTrue(members.contains(a1));
        assertFalse(members.contains(a2));
    }

    public void testViewChangeReceptionOnChannelCloseByCoordinator() throws Exception {
        Address  a1, a2;
        Vector members;
        Object obj;
        View v;

        c1=new JChannel(props);
        c1.connect("X");
        Util.sleep(500); // time to receive its own view
        dumpMessages("c1", c1);
        a1=c1.getLocalAddress();

        c2=new JChannel(props);
        c2.connect("X");
        Util.sleep(500); // time to receive its own view
        a2=c2.getLocalAddress();
        v=(View)c2.receive(1);
        members=v.getMembers();
        assertEquals(2, members.size());
        assertTrue(members.contains(a2));

        c1.close();
        Util.sleep(500);

        System.out.println("queue of c2 is " + c2.dumpQueue());
        assertTrue("found 0 messages in channel", c2.getNumMessages() > 0);
        obj=c2.receive(0);
        assertTrue(obj instanceof View);
        v=(View)obj;
        members=v.getMembers();
        assertEquals(1, members.size());
        assertFalse(members.contains(a1));
        assertTrue(members.contains(a2));

        assertEquals(0, c2.getNumMessages());
    }

    private void dumpMessages(String msg, JChannel ch) throws Exception {
        while(ch.getNumMessages() > 0) {
            Object obj=ch.receive(0);
            if(obj instanceof View)
                System.out.println(msg + ": " + obj);
        }
    }

    public void testConnectDisconnectConnectCloseSequence() throws ChannelException {
        System.out.println("-- creating channel --");
        channel=new JChannel(props);
        System.out.println("-- connecting channel to CloseTest1--");
        channel.connect("CloseTest1");
        System.out.println("view is " + channel.getView());
        System.out.println("-- disconnecting channel --");
        channel.disconnect();
        System.out.println("-- connecting channel to OtherGroup --");
        channel.connect("OtherGroup");
        System.out.println("view is " + channel.getView());
        System.out.println("-- closing channel --");
        channel.close();
        System.out.println("-- done, threads are ");
        Util.printThreads();
    }


    public void testConnectCloseSequenceWith2Members() throws ChannelException {
        System.out.println("-- creating channel --");
        channel=new JChannel(props);
        System.out.println("-- connecting channel --");
        channel.connect("X");
        System.out.println("view is " + channel.getView());

        System.out.println("-- creating channel1 --");
        channel1=new JChannel(props);
        System.out.println("-- connecting channel1 --");
        channel1.connect("X");
        System.out.println("view is " + channel1.getView());

        System.out.println("-- closing channel1 --");
        channel1.close();

        Util.sleep(2000);

        System.out.println("-- closing channel --");
        channel.close();



        System.out.println("-- done, threads are ");
        Util.printThreads();
    }


    public void testCreationAndClose2() throws Exception {
        System.out.println("-- creating channel2 --");
        channel2=new JChannel(props);
        System.out.println("-- connecting channel2 --");
        channel2.connect("CloseTest2");
        System.out.println("-- closing channel --");
        channel2.close();
        Util.sleep(2000);
        Util.printThreads();
    }


    public void testChannelClosedException() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel(props);
        System.out.println("-- connecting channel --");
        channel.connect("CloseTestLoop");
        System.out.println("-- closing channel --");
        channel.close();
        Util.sleep(2000);

        try {
            channel.connect("newGroup");
            fail(); // cannot connect to a closed channel
        }
        catch(ChannelClosedException ex) {
            assertTrue(true);
        }
    }

    public void testCreationAndCloseLoop() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel(props);

        for(int i=1; i <= 10; i++) {
            System.out.println("-- connecting channel (attempt #" + i + " ) --");
            channel.connect("CloseTestLoop2");
            System.out.println("-- closing channel --");
            channel.close();

            System.out.println("-- reopening channel --");
            channel.open();
        }
        channel.close();
    }

    public void testShutdown() throws Exception {
        System.out.println("-- creating channel --");
        channel=new JChannel(props);
        System.out.println("-- connecting channel --");
        channel.connect("bla");
        System.out.println("-- shutting down channel --");
        channel.shutdown();

        Thread threads[]=new Thread[Thread.activeCount()];
        Thread.enumerate(threads);
        System.out.println("-- active threads:");
        for(int i=0; i < threads.length; i++)
            System.out.println(threads[i]);
        assertTrue(threads.length < 5);
    }


     public void testMultipleConnectsAndDisconnects() throws Exception {
        c1=new JChannel(props);
        assertTrue(c1.isOpen());
        assertFalse(c1.isConnected());
        c1.connect("bla");
        assertTrue(c1.isOpen());
        assertTrue(c1.isConnected());
        assertServiceAndClusterView(c1, 1);

        c2=new JChannel(props);
        assertTrue(c2.isOpen());
        assertFalse(c2.isConnected());

        c2.connect("bla");
        assertTrue(c2.isOpen());
        assertTrue(c2.isConnected());
        assertServiceAndClusterView(c2, 2);
        Util.sleep(500);
        assertServiceAndClusterView(c1, 2);

        c2.disconnect();
        assertTrue(c2.isOpen());
        assertFalse(c2.isConnected());
        Util.sleep(500);
        assertServiceAndClusterView(c1, 1);

        c2.connect("bla");
        assertTrue(c2.isOpen());
        assertTrue(c2.isConnected());
        assertServiceAndClusterView(c2, 2);
        Util.sleep(300);
        assertServiceAndClusterView(c1, 2);

        // Now see what happens if we reconnect the first channel
        c3=new JChannel(props);
        assertTrue(c3.isOpen());
        assertFalse(c3.isConnected());
        assertServiceAndClusterView(c1, 2);
        assertServiceAndClusterView(c2, 2);

        c1.disconnect();
        assertTrue(c1.isOpen());
        assertFalse(c1.isConnected());
        assertServiceAndClusterView(c2, 1);
        assertTrue(c3.isOpen());
        assertFalse(c3.isConnected());

        c1.connect("bla");
        assertTrue(c1.isOpen());
        assertTrue(c1.isConnected());
        assertServiceAndClusterView(c1, 2);
        Util.sleep(500);
        assertServiceAndClusterView(c2, 2);
        assertTrue(c3.isOpen());
        assertFalse(c3.isConnected());
    }


    private void assertServiceAndClusterView(Channel ch, int num) {
           View view=ch.getView();
           String msg="view=" + view;
           assertNotNull(view);
           assertEquals(msg, num, view.size());
       }



    public static void main(String[] args) {
        String[] testCaseName={CloseTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
