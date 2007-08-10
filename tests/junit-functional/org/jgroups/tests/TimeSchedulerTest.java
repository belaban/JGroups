package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.TimeoutException;
import org.jgroups.stack.Interval;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.Promise;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;


/**
 * Test cases for TimeScheduler
 * @author Bela Ban
 * @version $Id: TimeSchedulerTest.java,v 1.2 2007/08/10 12:32:14 belaban Exp $
 */
public class TimeSchedulerTest extends TestCase {
    TimeScheduler timer=null;
    static final int NUM_MSGS=1000;
    static long[] xmit_timeouts={1000, 2000, 4000, 8000};
    static double PERCENTAGE_OFF=0.3; // how much can expected xmit_timeout and real timeout differ to still be okay ?
    HashMap<Long,Entry> msgs=new HashMap<Long,Entry>(); // keys=seqnos (Long), values=Entries


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
        }
        catch(InterruptedException e) {
        }
    }


    public void testCancel() throws InterruptedException {
        for(int i=0; i < 10; i++)
            timer.scheduleWithDynamicInterval(new OneTimeTask(1000));
        assertEquals(10, timer.size());
        timer.stop();
        assertEquals(0, timer.size());
    }


    public void testTaskCancellationBeforeTaskHasRun() {
        Future future;
        StressTask task=new StressTask();
        future=timer.scheduleWithDynamicInterval(task);
        assertEquals(1, timer.size());
        future.cancel(true);
        assertEquals(1, timer.size());

        Util.sleep(200);
        int num_executions=task.getNum_executions();
        System.out.println("number of task executions=" + num_executions);
        assertEquals("task should never have executed as it was cancelled before execution", 0, num_executions);

        timer.purge(); // removes cancelled tasks
        assertEquals(0, timer.size());
    }

    public void testTaskCancellationAfterHasRun() {
        Future future;
        StressTask task=new StressTask();
        future=timer.scheduleWithDynamicInterval(task);
        assertEquals(1, timer.size());

        Util.sleep(200); // wait until task has executed
        future.cancel(true);
        assertEquals(1, timer.size());

        int num_executions=task.getNum_executions();
        System.out.println("number of task executions=" + num_executions);
        assertTrue("task should have executed at least 1 time, as it was cancelled after 200ms", num_executions >= 1);
        timer.purge(); // removes cancelled tasks
        assertEquals(0, timer.size());
    }



    public void testRepeatingTask() {
        Future future;
        System.out.println(System.currentTimeMillis() + ": adding task");
        RepeatingTask task=new RepeatingTask(500);
        future=timer.scheduleWithDynamicInterval(task);
        Util.sleep(5000);

        System.out.println("<<< cancelling task");
        future.cancel(true);
        Util.sleep(2000);
        int num=task.getNum();
        System.out.println("task executed " + num + " times");
        assertTrue(num >= 9 && num < 11);
    }

    public void testStress() {
        StressTask t;

        for(int i=0; i < 1000; i++) {
            for(int j=0; j < 1000; j++) {
                t=new StressTask();
                Future future=timer.scheduleWithDynamicInterval(t);
                future.cancel(true);
            }
            System.out.println(i + ": " + timer.size());
        }
        for(int i=0; i < 10; i++) {
            System.out.println(timer.size());
            Util.sleep(500);
        }
        assertEquals(0, timer.size());
    }


    public void testDynamicTask() throws InterruptedException {
        TimeScheduler.Task task=new DynamicTask();
        ScheduledFuture<?> future=timer.scheduleWithDynamicInterval(task);
        assertEquals(1, timer.getQueue().size());

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        Thread.sleep(3000);
        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(true);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }



    public void testDynamicTaskCancel() throws InterruptedException {

        TimeScheduler.Task task=new DynamicTask();
        ScheduledFuture<?> future=timer.scheduleWithDynamicInterval(task);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        Thread.sleep(3000);
        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        boolean success=future.cancel(true);
        assertTrue(success);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());

        success=future.cancel(true);
        assertTrue(success); // we try to cancel an already cancelled task
    }


    public void testIsDone() throws InterruptedException {
        TimeScheduler.Task task=new DynamicTask();
        ScheduledFuture<?> future=timer.scheduleWithDynamicInterval(task);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        Thread.sleep(3000);
        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        future.cancel(true);
        assertTrue(future.isCancelled());
        assertTrue(future.isDone());
    }

    public void testIsDone2() throws InterruptedException {
        TimeScheduler.Task task=new DynamicTask(new long[]{1000,2000,-1});
        ScheduledFuture<?> future=timer.scheduleWithDynamicInterval(task);

        assertFalse(future.isCancelled());
        assertFalse(future.isDone());

        Thread.sleep(3500);
        assertFalse(future.isCancelled());
        assertTrue(future.isDone());

        boolean success=future.cancel(true);
        if(success)
            assertTrue(future.isCancelled());
        else
            assertFalse(future.isCancelled());
        assertTrue(future.isDone());
    }


    public void testIsDone3() throws InterruptedException {
        TimeScheduler.Task task=new DynamicTask(new long[]{-1});
        ScheduledFuture<?> future=timer.scheduleWithDynamicInterval(task);
        Thread.sleep(100);
        assertFalse(future.isCancelled());
        assertTrue(future.isDone());

        boolean success=future.cancel(true);
        if(success)
            assertTrue(future.isCancelled());
        else
            assertFalse(future.isCancelled());
        assertTrue(future.isDone());
    }


    public void testImmediateExecution() {
        Promise p=new Promise();
        ImmediateTask task=new ImmediateTask(p);
        timer.execute(task);
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


    public void test2Tasks() throws InterruptedException {
         int size;

         System.out.println(System.currentTimeMillis() + ": adding task");
         timer.schedule(new MyTask(), 500, TimeUnit.MILLISECONDS);
         size=timer.size();
         System.out.println("queue size=" + size);
         assertEquals(1, size);
         Thread.sleep(1000);
         size=timer.size();
         System.out.println("queue size=" + size);
         assertEquals(0, size);

         Thread.sleep(1500);
         System.out.println(System.currentTimeMillis() + ": adding task");
         timer.schedule(new MyTask(), 500, TimeUnit.MILLISECONDS);

         System.out.println(System.currentTimeMillis() + ": adding task");
         timer.schedule(new MyTask(), 500, TimeUnit.MILLISECONDS);

         System.out.println(System.currentTimeMillis() + ": adding task");
         timer.schedule(new MyTask(), 500, TimeUnit.MILLISECONDS);

         size=timer.size();
         System.out.println("queue size=" + size);
         assertEquals(3, size);

         Thread.sleep(1000);
         size=timer.size();
         System.out.println("queue size=" + size);
         assertEquals(0, size);
     }





     /**
      * Tests whether retransmits are called at correct times for 1000 messages. A retransmit should not be
      * more than 30% earlier or later than the scheduled retransmission time
      */
     public void testRetransmits() throws InterruptedException {
         Entry entry;
         int num_non_correct_entries=0;

         // 1. Add NUM_MSGS messages:
         System.out.println("-- adding " + NUM_MSGS + " messages:");
         for(long i=0; i < NUM_MSGS; i++) {
             entry=new Entry(i);
             msgs.put(new Long(i), entry);
             timer.scheduleWithDynamicInterval(entry);
         }
         System.out.println("-- done");

         // 2. Wait for at least 4 xmits/msg: total of 1000 + 2000 + 4000 + 8000ms = 15000ms; wait for 20000ms
         System.out.println("-- waiting for 20 secs for all retransmits");
         Thread.sleep(20000);

         // 3. Check whether all Entries have correct retransmission times
         for(long i=0; i < NUM_MSGS; i++) {
             entry=msgs.get(new Long(i));
             if(!entry.isCorrect()) {
                 num_non_correct_entries++;
             }
         }

         if(num_non_correct_entries > 0)
             System.err.println("Number of incorrect retransmission timeouts: " + num_non_correct_entries);
         else {
             for(long i=0; i < NUM_MSGS; i++) {
                 entry=msgs.get(new Long(i));
                 if(entry != null)
                     System.out.println(i + ": " + entry);
             }
         }
         assertEquals(0, num_non_correct_entries);
     }




    static private class ImmediateTask implements Runnable {
        Promise p;

        public ImmediateTask(Promise p) {
            this.p=p;
        }

        public void run() {
            p.setResult(Boolean.TRUE);
        }
    }

    static class MyTask implements Runnable {
        public void run() {
            System.out.println(System.currentTimeMillis() + ": this is MyTask running - done");
        }
    }


    static class DynamicTask implements TimeScheduler.Task {
        long[] times={1000,2000,4000};
        int index=0;

        public DynamicTask() {
        }

        public DynamicTask(long[] times) {
            this.times=times;
        }

        public long nextInterval() {
            if(index == times.length -1)
                return times[index];
            else
                return times[index++];
        }

        public void run() {
            System.out.println("dynamic task run at " + new Date());
        }
    }



    static class OneTimeTask implements TimeScheduler.Task {
        boolean done=false;
        private long timeout=0;

        OneTimeTask(long timeout) {
            this.timeout=timeout;
        }

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

        public int getNum_executions() {
            return num_executions;
        }

        public long nextInterval() {
            return 50;
        }

        public void run() {
            num_executions++;
        }
    }



    private static class Entry implements TimeScheduler.Task {
        long start_time=0;  // time message was added
        long first_xmit=0;  // time between start_time and first_xmit should be ca. 1000ms
        long second_xmit=0; // time between first_xmit and second_xmit should be ca. 2000ms
        long third_xmit=0;  // time between third_xmit and second_xmit should be ca. 4000ms
        long fourth_xmit=0; // time between third_xmit and second_xmit should be ca. 8000ms
        boolean cancelled=false;
        Interval interval=new StaticInterval(xmit_timeouts);
        long seqno=0;


        Entry(long seqno) {
            this.seqno=seqno;
            start_time=System.currentTimeMillis();
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
            StringBuilder sb=new StringBuilder();
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
