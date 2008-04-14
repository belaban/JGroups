package org.jgroups.tests;

import org.testng.annotations.*;
import org.jgroups.Channel;
import org.jgroups.JChannelFactory;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.mux.MuxChannel;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.*;

/**
 * Test the multiplexer concurrency functionality. This is described in http://jira.jboss.com/jira/browse/JGRP-426
 * @author Bela Ban
 * @version $Id: MultiplexerConcurrentTest.java,v 1.7 2008/04/14 07:54:06 belaban Exp $
 */
public class MultiplexerConcurrentTest extends ChannelTestBase {
    private Channel s1, s2, s11, s21;
    JChannelFactory factory, factory2;

    private static final long MIN_TIME=1000; // 1 sec between msgs
    private static final long MAX_TIME=5000;



    @BeforeMethod
    public void setUp() throws Exception {
        factory=new JChannelFactory();
        factory.setMultiplexerConfig(mux_conf);

        factory2=new JChannelFactory();
        factory2.setMultiplexerConfig(mux_conf);
    }

    @AfterMethod
    public void tearDown() throws Exception {
        if(s1 != null)
            s1.close();
        if(s2 != null)
            s2.close();

        if(s21 != null) {
            s21.close();
            s21=null;
        }
        if(s11 != null) {
            s11.close();
            s11=null;
        }
        if(s1 != null) {
            assertFalse(((MuxChannel)s1).getChannel().isOpen());
            assertFalse(((MuxChannel)s1).getChannel().isConnected());
        }
        if(s2 != null) {
            assertFalse(((MuxChannel)s2).getChannel().isOpen());
            assertFalse(((MuxChannel)s2).getChannel().isConnected());
        }
        s1=s2=null;
    }


    /** Use case #1 in http://jira.jboss.com/jira/browse/JGRP-426:<br/>
     * Sender A sends M1 to S1 and M2 to S1. M2 should wait until M1 is done
     */
    @Test
    public void testTwoMessagesFromSameSenderToSameService() throws Exception {
        final MyReceiver receiver=new MyReceiver();
        s1=factory.createMultiplexerChannel(mux_conf_stack, "s1");
        s1.connect("bla");
        s1.setReceiver(receiver);
        s1.send(null, null, "slow");
        s1.send(null, null, "fast");
        synchronized(receiver) {
            while(!receiver.done())
                receiver.wait();
        }

        // verify time diffs
        Map<Long,Message> results=receiver.getMessages();
        System.out.println("results:\n" + printMessages(results));
        Iterator<Map.Entry<Long,Message>> it=results.entrySet().iterator();
        long    time;
        Message msg;
        Map.Entry<Long,Message> entry;
        entry=it.next();
        time=entry.getKey();
        msg=entry.getValue();
        String mode=(String)msg.getObject();
        org.testng.Assert.assertEquals(mode, "slow", "the slow message needs to be delivered before the fast one");
        entry=it.next();
        long time2=entry.getKey();
        long diff=Math.abs(time2-time);
        System.out.println("diff=" + diff);
        assertTrue(diff >= MAX_TIME && diff < 6000);
    }


    /** Use case #2 in http://jira.jboss.com/jira/browse/JGRP-426:<br/>
      * Sender A sends M1 to S1 and M2 to S2. M2 should get processed immediately and not
     * have to wait for M1 to complete
      */
     @Test
     public void testTwoMessagesFromSameSenderToDifferentServices() throws Exception {
        final MyReceiver receiver=new MyReceiver();
        s1=factory.createMultiplexerChannel(mux_conf_stack, "s1");
        s1.connect("bla");
        s1.setReceiver(receiver);

        s2=factory.createMultiplexerChannel(mux_conf_stack, "s2");
        s2.connect("bla");
        s2.setReceiver(receiver);

        s1.send(null, null, "slow");
        Util.sleep(200);
        s2.send(null, null, "fast");
        synchronized(receiver) {
            while(!receiver.done())
                receiver.wait();
        }

        // verify time diffs
        Map<Long,Message> results=receiver.getMessages();
        System.out.println("results:\n" + printMessages(results));
        Set<Long> times=results.keySet();


        Iterator<Long> it=times.iterator();
        long    time, time2, diff;
        time=it.next();
        time2=it.next();
        diff=Math.abs(time2-time);
        System.out.println("diff=" + diff);
        assertTrue("failing as we don't yet have concurrent delivery", diff < MIN_TIME);
    }


    /**
     * Use case #3 in http://jira.jboss.com/jira/browse/JGRP-426:<br/>
     * Sender A sends M1 to S1 and sender B sends M2 to S1. M2 should get processed concurrently to M1
     * and should not have to wait for M1's completion
     */
    @Test
    public void  testTwoMessagesFromDifferentSendersToSameService() throws Exception {
        final MyReceiver receiver=new MyReceiver();
        s1=factory.createMultiplexerChannel(mux_conf_stack, "s1");
        s1.connect("bla");
        s1.setReceiver(receiver);

        s2=factory2.createMultiplexerChannel(mux_conf_stack, "s1"); // same service
        s2.connect("bla");

        s1.send(null, null, "slow");
        Util.sleep(200); // the slower message needs to be received first
        s2.send(null, null, "fast");
        synchronized(receiver) {
            while(!receiver.done())
                receiver.wait();
        }
         // verify time diffs
        Map<Long,Message> results=receiver.getMessages();
        System.out.println("results:\n" + printMessages(results));
        Set<Long> times=results.keySet();

        Iterator<Long> it=times.iterator();
        long    time, time2, diff;
        time=it.next();
        time2=it.next();
        diff=Math.abs(time2-time);
        System.out.println("diff=" + diff);
        assertTrue("failing as we don't yet have concurrent delivery", diff < MIN_TIME);
    }

    /**
     * Use case #4 in http://jira.jboss.com/jira/browse/JGRP-426:<br/>
     * Sender A sends M1 to S1 and sender B sends M2 to S2. M1 and M2 should get processed concurrently
     */
    @Test
    public void testTwoMessagesFromDifferentSendersToDifferentServices() throws Exception {
        final MyReceiver receiver=new MyReceiver();
        s1=factory.createMultiplexerChannel(mux_conf_stack, "s1");
        s1.connect("bla");
        s1.setReceiver(receiver);
        s11=factory.createMultiplexerChannel(mux_conf_stack, "s2");
        s11.connect("bla");
        s11.setReceiver(receiver);


        s2=factory2.createMultiplexerChannel(mux_conf_stack, "s1");
        s2.connect("bla");

        s21=factory2.createMultiplexerChannel(mux_conf_stack, "s2");
        s21.connect("bla");

        s1.send(null, null, "slow");
        Util.sleep(200); // the slower message needs to be received first
        s21.send(null, null, "fast");
        synchronized(receiver) {
            while(!receiver.done())
                receiver.wait();
        }
         // verify time diffs
        Map<Long,Message> results=receiver.getMessages();
        System.out.println("results:\n" + printMessages(results));
        Set<Long> times=results.keySet();

        Iterator<Long> it=times.iterator();
        long    time, time2, diff;
        time=it.next();
        time2=it.next();
        diff=Math.abs(time2-time);
        System.out.println("diff=" + diff);
        assertTrue("failing as we don't yet have concurrent delivery", diff < MIN_TIME);
    }

    

    private static class MyReceiver extends ReceiverAdapter {
        final Map<Long,Message> msgs=new HashMap<Long,Message>();


        public void receive(Message msg) {
            String mode=(String)msg.getObject();
            System.out.println("received " + msg + " (" + mode + ")");
            msgs.put(System.currentTimeMillis(), msg);
            if(mode.equalsIgnoreCase("slow")) {
                System.out.println("sleeping for 5 secs");
                Util.sleep(5000);
            }
            synchronized(this) {
                if(msgs.size() == 2)
                    this.notify();
            }
        }

        public boolean done() {
            synchronized(msgs) {
                return msgs.size() == 2;
            }
        }

        public Map<Long,Message> getMessages() {
            return new TreeMap<Long,Message>(msgs);
        }
    }


    String printMessages(Map<Long,Message> map) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Long,Message> entry: map.entrySet()) {
            sb.append(new Date(entry.getKey())).append(": ").append(entry.getValue().getObject()).append("\n");
        }
        return sb.toString();
    }





}
