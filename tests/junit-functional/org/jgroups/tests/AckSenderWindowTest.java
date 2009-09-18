package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.stack.AckSenderWindow;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.Util;
import org.jgroups.util.TimeScheduler;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.AfterMethod;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Test cases for AckSenderWindow
 * @author Bela Ban
 * @version  $Id: AckSenderWindowTest.java,v 1.9 2009/09/18 10:51:31 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class AckSenderWindowTest {
    AckSenderWindow           win=null;
    static final int          NUM_MSGS=100;
    final static long[]       xmit_timeouts={1000, 2000, 4000, 8000};
    static final double       PERCENTAGE_OFF=1.3; // how much can expected xmit_timeout and real timeout differ to still be okay ?
    final Map<Long,Entry>     msgs=new ConcurrentHashMap<Long,Entry>(); // keys=seqnos (Long), values=Entries
    protected TimeScheduler   timer=null;

    @BeforeMethod
    protected void setUp() throws Exception {
        timer=new TimeScheduler(10);
        win=new AckSenderWindow(new MyRetransmitCommand(), new StaticInterval(xmit_timeouts), timer);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        timer.stop();
        win.reset();
    }


    public void testSimpleAdd() throws InterruptedException {
        for(int i=1; i <=5; i++)
            win.add(i, new Message());
        System.out.println("win = " + win);
        assert win.size() == 5;
        win.ack(1);
        System.out.println("win = " + win);
        assert win.size() == 4;
        win.ack(4);
        System.out.println("win = " + win);
        assert win.size() == 1;
        win.ack(44);
        assert win.size() == 0;
    }


    /** Tests whether retransmits are called at correct times for 1000 messages */
    @Test(groups=Global.FUNCTIONAL)
    public void testRetransmits() throws InterruptedException {
        int   num_non_correct_entries=0;

        // 1. Send NUM_MSGS messages:
        System.out.println("-- sending " + NUM_MSGS + " messages:");
        for(long i=0; i < NUM_MSGS; i++) {
            msgs.put(new Long(i), new Entry());
            win.add(i, new Message());
        }
        System.out.println("-- done");

        // 2. Wait for at least 4 xmits/msg: total of 1000 + 2000 + 4000 + 8000ms = 15000ms; wait for 20000ms
        System.out.println("-- waiting for all retransmits");
        long end_time=System.currentTimeMillis() + 20000L, curr, start=System.currentTimeMillis();

        Util.sleep(1000);
        while((curr=System.currentTimeMillis()) < end_time) {
            // 3. Check whether all Entries have correct retransmission times
            num_non_correct_entries=checkEntries(false);
            if(num_non_correct_entries == 0)
                break;
            Util.sleep(2000L);
        }

        System.out.println("-- waited for " + (System.currentTimeMillis() - start) + " ms");

        num_non_correct_entries=checkEntries(true);
        if(num_non_correct_entries > 0)
            System.err.println("Number of incorrect retransmission timeouts: " + num_non_correct_entries);
        assert num_non_correct_entries == 0;

    }



    public void testLowest() {
        for(long i=1; i < 5; i++)
            win.add(i, new Message());
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assert win.getLowest() == Global.DEFAULT_FIRST_UNICAST_SEQNO;

        win.ack(3);
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assert win.getLowest() == 4;

        win.ack(4);
        System.out.println("win = " + win + ", lowest=" + win.getLowest());
        assert win.getLowest() == 5;

        win.ack(2);
        assert win.getLowest() == 5;
    }

    public void testGetLowestMessage() {
        long[] seqnos={1,2,3,4,5};
        final Message[] messages=new Message[]{new Message(),new Message(),new Message(),new Message(),new Message()};

        for(int i=0; i < seqnos.length; i++) {
            win.add(seqnos[i], messages[i]);
        }
        System.out.println("win = " + win);

        Message msg=win.getLowestMessage();
        assert messages[0] == msg;

        win.ack(2);
        msg=win.getLowestMessage();
        assert messages[2] == msg;

        win.ack(7);
        msg=win.getLowestMessage();
        assert msg == null;
    }


    public void testAdd() {
        for(int i=1; i <= 10; i++)
            win.add(i, new Message());
        System.out.println("win = " + win);
        assert win.size() == 10;
        win.ack(7);
        assert win.size() == 3;
    }


    public void testAck() {
        for(int i=1; i <= 3; i++)
            win.add(i, new Message());
        assert win.size() == 3;
        win.ack(1);
        assert win.size() == 2;
        win.ack(2);
        assert win.size() == 1;
        win.ack(3);
        assert win.size() == 0;
    }
    


    int checkEntries(boolean print) {
        int retval=0;
        Entry entry;
        for(long i=0; i < NUM_MSGS; i++) {
            entry=msgs.get(new Long(i));
            if(!entry.isCorrect(i, print)) {
                retval++;
            }
        }
        return retval;
    }


    static class Entry {
        long start_time=0;  // time message was added
        long first_xmit=0;  // time between start_time and first_xmit should be ca. 1000ms
        long second_xmit=0; // time between first_xmit and second_xmit should be ca. 2000ms
        long third_xmit=0;  // time between third_xmit and second_xmit should be ca. 4000ms
        long fourth_xmit=0; // time between third_xmit and second_xmit should be ca. 8000ms

        Entry() {
            start_time=System.currentTimeMillis();
        }

        /** Entry is correct if xmit timeouts are not more than 30% off the mark */
        boolean isCorrect(long seqno, boolean print) {
            long    t;
            long    expected;
            long    diff, delta;

            t=first_xmit - start_time;
            expected=xmit_timeouts[0];
            diff=Math.abs(expected - t);
            delta=(long)(expected * PERCENTAGE_OFF);
            if(diff <= delta) return true;

            t=second_xmit - first_xmit;
            expected=xmit_timeouts[1];
            diff=Math.abs(expected - t);
            delta=(long)(expected * PERCENTAGE_OFF);
            if(diff <= delta) return true;

            t=third_xmit - second_xmit;
            expected=xmit_timeouts[2];
            diff=Math.abs(expected - t);
            delta=(long)(expected * PERCENTAGE_OFF);
            if(diff <= delta) return true;

            t=fourth_xmit - third_xmit;
            expected=xmit_timeouts[3];
            diff=Math.abs(expected - t);
            delta=(long)(expected * PERCENTAGE_OFF);
            if(diff <= delta) return true;

            if(print) {
                System.err.println("#" + seqno + ": " + this + ": (" + "entry is more than " +
                        PERCENTAGE_OFF + " percentage off ");
                return false;
            }
            return true;
        }

        boolean isCorrect(long seqno) {
            return isCorrect(seqno, true);
        }



        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(first_xmit - start_time).append(", ").append(second_xmit-first_xmit).append(", ");
            sb.append(third_xmit-second_xmit).append(", ").append(fourth_xmit-third_xmit);
            return sb.toString();
        }
    }


    class MyRetransmitCommand implements AckSenderWindow.RetransmitCommand {
	
        public void retransmit(long seqno, Message msg) {
            Entry entry=msgs.get(seqno);
            if(entry != null) {
                if(entry.first_xmit == 0) {
                    entry.first_xmit=System.currentTimeMillis();
                    return;
                }
		
                if(entry.second_xmit == 0) {
                    entry.second_xmit=System.currentTimeMillis();
                    return;
                }

                if(entry.third_xmit == 0) {
                    entry.third_xmit=System.currentTimeMillis();
                    return;
                }

                if(entry.fourth_xmit == 0) {
                    entry.fourth_xmit=System.currentTimeMillis();
                }
            }
        }

    }




    
    

}
