// $Id: AckSenderWindowTest.java,v 1.2.2.2 2009/09/08 12:23:22 belaban Exp $
package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.stack.AckSenderWindow;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.Util;
import org.jgroups.util.TimeScheduler;

import java.util.HashMap;


/**
 * Test cases for AckSenderWindow
 * @author Bela Ban
 */
public class AckSenderWindowTest extends TestCase {
    AckSenderWindow win=null;
    final int NUM_MSGS=100;
    long[] xmit_timeouts={1000, 2000, 4000, 8000};
    double PERCENTAGE_OFF=1.3; // how much can expected xmit_timeout and real timeout differ to still be okay ?
    HashMap msgs=new HashMap(); // keys=seqnos (Long), values=Entries
    protected TimeScheduler timer=null;


    protected void setUp() throws Exception {
        super.setUp();
        timer=new TimeScheduler(10);
    }

    protected void tearDown() throws Exception {
        timer.stop();
        super.tearDown();
    }

    class Entry {
        long start_time=0;  // time message was added
        long first_xmit=0;  // time between start_time and first_xmit should be ca. 1000ms
        long second_xmit=0; // time between first_xmit and second_xmit should be ca. 2000ms
        long third_xmit=0;  // time between third_xmit and second_xmit should be ca. 4000ms
        long fourth_xmit=0; // time between third_xmit and second_xmit should be ca. 8000ms

        Entry() {
            start_time=System.currentTimeMillis();
        }

        /**
         * Entry is correct if xmit timeouts are not more than 30% off the mark
         */
        boolean isCorrect(long seqno) {
            long t;
            long expected;
            long diff, delta;
            boolean off=false;

            t=first_xmit - start_time;
            expected=xmit_timeouts[0];
            diff=Math.abs(expected - t);
            delta=(long)(expected * PERCENTAGE_OFF);
            if(diff >= delta) off=true;

            t=second_xmit - first_xmit;
            expected=xmit_timeouts[1];
            diff=Math.abs(expected - t);
            delta=(long)(expected * PERCENTAGE_OFF);
            if(diff >= delta) off=true;

            t=third_xmit - second_xmit;
            expected=xmit_timeouts[2];
            diff=Math.abs(expected - t);
            delta=(long)(expected * PERCENTAGE_OFF);
            if(diff >= delta) off=true;

            t=fourth_xmit - third_xmit;
            expected=xmit_timeouts[3];
            diff=Math.abs(expected - t);
            delta=(long)(expected * PERCENTAGE_OFF);
            if(diff >= delta) off=true;

            if(off) {
                System.err.println("#" + seqno + ": " + this + ": (" + "entry is more than " +
                        PERCENTAGE_OFF + " percentage off ");
                return false;
            }
            return true;
        }


        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(first_xmit - start_time).append(", ").append(second_xmit - first_xmit).append(", ");
            sb.append(third_xmit - second_xmit).append(", ").append(fourth_xmit - third_xmit);
            return sb.toString();
        }
    }


    class MyRetransmitCommand implements AckSenderWindow.RetransmitCommand {

        public void retransmit(long seqno, Message msg) {
            Entry entry=(Entry)msgs.get(new Long(seqno));

            // System.out.println(" -- retransmit(" + seqno + ")");

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
                    return;
                }
            }
        }

    }


    public AckSenderWindowTest(String name) {
        super(name);
    }


    /**
     * Tests whether retransmits are called at correct times for 1000 messages
     */
    public void testRetransmits() {
        Entry entry;
        int num_non_correct_entries=0;

        win=new AckSenderWindow(new MyRetransmitCommand(), new StaticInterval(xmit_timeouts), timer);

        // 1. Send NUM_MSGS messages:
        System.out.println("-- sending " + NUM_MSGS + " messages:");
        for(long i=0; i < NUM_MSGS; i++) {
            msgs.put(new Long(i), new Entry());
            win.add(i, new Message());
        }
        System.out.println("-- done");

        // 2. Wait for at least 4 xmits/msg: total of 1000 + 2000 + 4000 + 8000ms = 15000ms; wait for 20000ms
        System.out.println("-- waiting for 20 secs for all retransmits");
        Util.sleep(20000);

        // 3. Check whether all Entries have correct retransmission times
        for(long i=0; i < NUM_MSGS; i++) {
            entry=(Entry)msgs.get(new Long(i));
            if(!entry.isCorrect(i)) {
                num_non_correct_entries++;
            }
        }

        if(num_non_correct_entries > 0)
            System.err.println("Number of incorrect retransmission timeouts: " + num_non_correct_entries);
        assertTrue(num_non_correct_entries == 0);
        win.reset();
    }


    public static Test suite() {
        TestSuite suite;
        suite=new TestSuite(AckSenderWindowTest.class);
        return (suite);
    }

    public static void main(String[] args) {
        String[] name={AckSenderWindowTest.class.getName()};
        junit.textui.TestRunner.main(name);
    }
}
