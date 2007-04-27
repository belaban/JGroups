package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.TimeoutException;
import org.jgroups.stack.Interval;
import org.jgroups.util.Promise;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.HashMap;


/**
 * Test cases for TimeScheduler
 *
 * @author Bela Ban
 * @version $Id: TimeSchedulerTest.java,v 1.9.2.1 2007/04/27 06:26:39 belaban Exp $
 */
public class TimeSchedulerTest extends TestCase {
    TimeScheduler timer=null;
    static final int NUM_MSGS=1000;
    long[] xmit_timeouts={1000, 2000, 4000, 8000};
    double PERCENTAGE_OFF=0.3; // how much can expected xmit_timeout and real timeout differ to still be okay ?
    HashMap msgs=new HashMap(); // keys=seqnos (Long), values=Entries


    public TimeSchedulerTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        timer=new TimeScheduler();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        try {
            timer.stop();
            timer.cancel();
            timer=null;
        }
        catch(InterruptedException e) {
        }
    }


    public void testCancel() {
        for(int i=0; i < 10; i++)
            timer.add(new OneTimeTask(1000));
        assertEquals(10, timer.size());
        timer.cancel();
        assertEquals(0, timer.size());
    }


    public void testTaskCancellationBeforeTaskHasRun() {
        StressTask task=new StressTask();
        timer.add(task);
        assertEquals(1, timer.size());
        task.cancel();
        assertEquals(1, timer.size());

        Util.sleep(200);
        int num_executions=task.getNum_executions();
        System.out.println("number of task executions=" + num_executions);
        assertEquals("task should never have executed as it was cancelled before execution", 0, num_executions);

        timer.purge(); // removes cancelled tasks
        assertEquals(0, timer.size());
    }

    public void testTaskCancellationAfterHasRun() {
        StressTask task=new StressTask();
        timer.add(task);
        assertEquals(1, timer.size());

        Util.sleep(200); // wait until task has executed
        task.cancel();
        assertEquals(1, timer.size());

        int num_executions=task.getNum_executions();
        System.out.println("number of task executions=" + num_executions);
        assertTrue("task should have executed at least 1 time, as it was cancelled after 200ms", num_executions >= 1);
        Util.sleep(60); // wait until the task is run again and is then cancelled
        timer.purge(); // removes cancelled tasks
        assertEquals(0, timer.size());
    }


    public void testImmediateExecution() {
        Promise p=new Promise();
        ImmediateTask task=new ImmediateTask(p);
        timer.add(task);
        try {
            long start=System.currentTimeMillis(), stop;
            p.getResultWithTimeout(5);
            stop=System.currentTimeMillis();
            System.out.println("task took " + (stop-start) + "ms");
        }
        catch(TimeoutException e) {
            fail("ran into timeout - task should have executed immediately");
        }
    }



    public void testStress() {
        StressTask t;

        for(int i=0; i < 1000; i++) {
            for(int j=0; j < 1000; j++) {
                t=new StressTask();
                timer.add(t);
                t.cancel();
            }
            System.out.println(i + ": " + timer.size());
        }
        for(int i=0; i < 10; i++) {
            System.out.println(timer.size());
            Util.sleep(500);
        }
    }

    public void test2Tasks() {
        int size;

        System.out.println("timer: " + timer);

        System.out.println(System.currentTimeMillis() + ": adding task");
        timer.add(new OneTimeTask(500));
        size=timer.size();
        System.out.println("queue size=" + size);
        assertEquals(1, size);
        Util.sleep(1000);
        size=timer.size();
        System.out.println("queue size=" + size);
        assertEquals(0, size);

        Util.sleep(1500);
        System.out.println(System.currentTimeMillis() + ": adding task");
        timer.add(new OneTimeTask(500));

        System.out.println(System.currentTimeMillis() + ": adding task");
        timer.add(new OneTimeTask(500));

        System.out.println(System.currentTimeMillis() + ": adding task");
        timer.add(new OneTimeTask(500));

        size=timer.size();
        System.out.println("queue size=" + size);
        assertEquals(3, size);

        Util.sleep(1000);
        size=timer.size();
        System.out.println("queue size=" + size);
        assertEquals(0, size);
    }



    public void testRepeatingTask() {
        System.out.println(System.currentTimeMillis() + ": adding task");
        RepeatingTask task=new RepeatingTask(500);
        timer.add(task);
        Util.sleep(5000);

        System.out.println("<<< cancelling task");
        task.done();
        Util.sleep(2000);
        int num=task.getNum();
        System.out.println("task executed " + num + " times");
        assertTrue(num >= 9 && num < 11);
    }




    /**
     * Tests whether retransmits are called at correct times for 1000 messages. A retransmit should not be
     * more than 30% earlier or later than the scheduled retransmission time
     */
    public void testRetransmits() {
        Entry entry;
        int num_non_correct_entries=0;

        // 1. Add NUM_MSGS messages:
        System.out.println("-- adding " + NUM_MSGS + " messages:");
        for(long i=0; i < NUM_MSGS; i++) {
            entry=new Entry(i);
            msgs.put(new Long(i), entry);
            timer.add(entry);
        }
        System.out.println("-- done");

        // 2. Wait for at least 4 xmits/msg: total of 1000 + 2000 + 4000 + 8000ms = 15000ms; wait for 20000ms
        System.out.println("-- waiting for 20 secs for all retransmits");
        Util.sleep(20000);

        // 3. Check whether all Entries have correct retransmission times
        for(long i=0; i < NUM_MSGS; i++) {
            entry=(Entry)msgs.get(new Long(i));
            if(!entry.isCorrect()) {
                num_non_correct_entries++;
            }
        }

        if(num_non_correct_entries > 0)
            System.err.println("Number of incorrect retransmission timeouts: " + num_non_correct_entries);
        else {
            for(long i=0; i < NUM_MSGS; i++) {
                entry=(Entry)msgs.get(new Long(i));
                if(entry != null)
                    System.out.println(i + ": " + entry);
            }
        }
        assertEquals(0, num_non_correct_entries);
    }





    static private class ImmediateTask implements TimeScheduler.Task {
        Promise p;
        boolean executed=false;

        public ImmediateTask(Promise p) {
            this.p=p;
        }

        public boolean cancelled() {
            return executed;
        }

        public long nextInterval() {
            return 0;
        }

        public void run() {
            p.setResult(Boolean.TRUE);
            executed=true;
        }
    }


    static class OneTimeTask implements TimeScheduler.Task {
        boolean done=false;
        private long timeout=0;

        OneTimeTask(long timeout) {
            this.timeout=timeout;
        }

        public boolean cancelled() {
            return done;
        }

        public void done() {done=true;}

        public long nextInterval() {
            return timeout;
        }

        public void run() {
            System.out.println(System.currentTimeMillis() + ": this is MyTask running - done");
            done=true;
        }
    }


    static class RepeatingTask extends OneTimeTask {
        int num=0;

        RepeatingTask(long timeout) {
            super(timeout);
        }

        public int getNum() {
            return num;
        }

        public void run() {
            System.out.println((num +1) + ": this is the repeating task");
            num++;
        }
    }


    static class StressTask implements TimeScheduler.Task {
        boolean cancelled=false;
        int num_executions=0;

        public void cancel() {
            cancelled=true;
        }

        public boolean cancelled() {
            return cancelled;
        }

        public long nextInterval() {
            return 50;
        }

        public void run() {
            // System.out.println("executed");
            num_executions++;
        }

        public int getNum_executions() {
            return num_executions;
        }
    }

    class Entry implements TimeScheduler.Task {
        long start_time=0;  // time message was added
        long first_xmit=0;  // time between start_time and first_xmit should be ca. 1000ms
        long second_xmit=0; // time between first_xmit and second_xmit should be ca. 2000ms
        long third_xmit=0;  // time between third_xmit and second_xmit should be ca. 4000ms
        long fourth_xmit=0; // time between third_xmit and second_xmit should be ca. 8000ms
        boolean cancelled=false;
        Interval interval=new Interval(xmit_timeouts);
        long seqno=0;


        Entry(long seqno) {
            this.seqno=seqno;
            start_time=System.currentTimeMillis();
        }

        public void cancel() {
            cancelled=true;
        }

        public boolean cancelled() {
            return cancelled;
        }

        public long nextInterval() {
            return interval.next();
        }

        public void run() {
            if(first_xmit == 0)
                first_xmit=System.currentTimeMillis();
            else
                if(second_xmit == 0)
                    second_xmit=System.currentTimeMillis();
                else
                    if(third_xmit == 0)
                        third_xmit=System.currentTimeMillis();
                    else
                        if(fourth_xmit == 0)
                            fourth_xmit=System.currentTimeMillis();
        }


        /**
         * Entry is correct if xmit timeouts are not more than 30% off the mark
         */
        boolean isCorrect() {
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
            StringBuffer sb=new StringBuffer();
            sb.append(first_xmit - start_time).append(", ").append(second_xmit - first_xmit).append(", ");
            sb.append(third_xmit - second_xmit).append(", ").append(fourth_xmit - third_xmit);
            return sb.toString();
        }
    }


    public static Test suite() {
        TestSuite suite;
        suite=new TestSuite(TimeSchedulerTest.class);
        return (suite);
    }

    public static void main(String[] args) {
        String[] name={TimeSchedulerTest.class.getName()};
        junit.textui.TestRunner.main(name);
    }
}
