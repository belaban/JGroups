package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.TimeoutException;
import org.jgroups.stack.Interval;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.DefaultTimeScheduler;
import org.jgroups.util.Promise;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;


/**
 * Test cases for TimeScheduler
 * @author Bela Ban
 */
@Test(groups=Global.TIME_SENSITIVE,dataProvider="createTimer",sequential=true)
public class TimeSchedulerTest {
    static final int NUM_MSGS=1000;
    static long[] xmit_timeouts={1000, 2000, 4000, 8000};
    static double PERCENTAGE_OFF=0.3; // how much can expected xmit_timeout and real timeout differ to still be okay ?


    @DataProvider(name="createTimer")
    Object[][] createTimer() {
        return Util.createTimer();
    }


    @Test(dataProvider="createTimer")
    public void testCancel(TimeScheduler timer) throws InterruptedException {
        for(int i=0; i < 10; i++)
            timer.scheduleWithDynamicInterval(new OneTimeTask(1000));
        assert timer.size() == 10;
        timer.stop();
        assert timer.size() == 0 : "stopped timer should have no tasks";
    }


    /**
     * Tests creating many tasks at the same time and then cancelling every second task. Asserts that not all tasks are cancelled;
     * this was the case in an early implementation of {@link org.jgroups.util.TimeScheduler2}.
     * @param timer
     */
    @Test(dataProvider="createTimer")
    public void testSchedulingTasksThenCancellingEverySecondTask(TimeScheduler timer) {
        final int NUM=20;

        Future<?>[] futures=new Future<?>[NUM];

        try {
            for(int i=0; i < NUM; i++) {
                futures[i]=timer.schedule(new MyRunnable(i), 1000, TimeUnit.MILLISECONDS);
                if(i % 2 == 0)
                    futures[i].cancel(false);
            }

            Util.sleep(3000);

            for(int i=0; i < NUM; i++) {
                Future<?> future=futures[i];
                System.out.println("[" + i + "] done=" + future.isDone() + ", cancelled=" + future.isCancelled());
            }

            for(int i=0; i < NUM; i++) {
                Future<?> future=futures[i];
                assert future.isDone();
                if(i % 2 == 0)
                    assert future.isCancelled();
                else
                    assert !future.isCancelled();
            }
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testTaskCancellationBeforeTaskHasRun(TimeScheduler timer) throws InterruptedException {
        Future future;
        StressTask task=new StressTask();
        try {
            future=timer.scheduleWithDynamicInterval(task);
            assert timer.size() == 1;
            future.cancel(true);
            assert timer.size() == 1;
            Util.sleep(200);
            int num_executions=task.getNumExecutions();
            System.out.println("number of task executions=" + num_executions);
            assert num_executions ==0 : "task should never have executed as it was cancelled before execution";
            if(timer instanceof DefaultTimeScheduler)
                ((DefaultTimeScheduler)timer).purge(); // removes cancelled tasks
            else
                Util.sleep(1000);
            assert timer.size() == 0;
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testTaskCancellationAfterHasRun(TimeScheduler timer) throws InterruptedException {
        Future future;
        StressTask task=new StressTask();
        try {
            future=timer.scheduleWithDynamicInterval(task);
            assert timer.size() == 1;

            Util.sleep(500); // wait until task has executed
            future.cancel(true);
            int size=timer.size();
            assert size == 1 : " timer size should be 1, but is " + size;

            int num_executions=task.getNumExecutions();
            System.out.println("number of task executions=" + num_executions);
            assert num_executions >= 1 : "task should have executed at least 1 time, as it was cancelled after 500ms";
            if(timer instanceof DefaultTimeScheduler)
                ((DefaultTimeScheduler)timer).purge(); // removes cancelled tasks
            else
                Util.sleep(1000);
            assert timer.size() == 0 : " timer size should be 0, but is " + size;
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testRepeatingTask(TimeScheduler timer) throws InterruptedException {
        Future future;
        RepeatingTask task=new RepeatingTask(300);
        try {
            future=timer.scheduleAtFixedRate(task, 0, 300, TimeUnit.MILLISECONDS);
            Util.sleep(3200);

            System.out.println("<<< cancelling task");
            future.cancel(true);
            Util.sleep(1000);
            int num=task.getNum();
            System.out.println("task executed " + num + " times");
            
            assert num >= 8 && num <= 11 : "number of executions is " + num + ", but should be >= 8 and <= 11\n" +
                    "Execution times: " + printExecutionTimes(task);
        }
        finally {
            timer.stop();
        }
    }

    private static String printExecutionTimes(RepeatingTask task) {
        StringBuilder sb=new StringBuilder();
        List<Long> times=task.getExecutionTimes();
        if(times.isEmpty())
            return "[]";
        int cnt=1;
        for(Long time: times) {
            sb.append("#" + cnt++ + ": ").append(time).append("\n");
        }
        return sb.toString();
    }


    @Test(dataProvider="createTimer")
    public void testStress(TimeScheduler timer) throws InterruptedException {
        StressTask t;
        final int NUM_A=500, NUM_B=1000;
        int cnt=0, print=NUM_A * NUM_B / 10;
        try {
            System.out.println("-- adding "+ (NUM_A * NUM_B) + " tasks...");
            for(int i=0; i < NUM_A; i++) {
                for(int j=0; j < NUM_B; j++) {
                    t=new StressTask();
                    Future future=timer.scheduleWithDynamicInterval(t);
                    future.cancel(true);
                    cnt++;
                    if(cnt > 0 && cnt % print == 0)
                        System.out.println(cnt);
                }
            }
            System.out.println("-- added "+ (NUM_A * NUM_B) + " tasks, waiting for termination");
            Util.sleep(1000);
            for(int i=0; i < 10; i++) {
                int size=timer.size();
                System.out.println(size);
                if(size == 0)
                    break;
                Util.sleep(500);
            }
            assert timer.size() == 0;
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testDynamicTask(TimeScheduler timer) throws InterruptedException {
        TimeScheduler.Task task=new DynamicTask();
        try {
            Future<?> future=timer.scheduleWithDynamicInterval(task);

            assert !(future.isCancelled());
            assert !(future.isDone());

            Thread.sleep(3000);
            assert !(future.isCancelled());
            assert !(future.isDone());

            future.cancel(true);
            assert future.isCancelled();
            assert future.isDone();
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testDynamicTaskCancel(TimeScheduler timer) throws InterruptedException {
        try {
            TimeScheduler.Task task=new DynamicTask();
            Future<?> future=timer.scheduleWithDynamicInterval(task);

            assert !(future.isCancelled());
            assert !(future.isDone());

            Thread.sleep(3000);
            assert !(future.isCancelled());
            assert !(future.isDone());

            assert future.cancel(true);
            assert future.isCancelled();
            assert future.isDone();

            assert future.cancel(true) == false;
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testIsDone(TimeScheduler timer) throws InterruptedException {
        try {
            TimeScheduler.Task task=new DynamicTask();
            Future<?> future=timer.scheduleWithDynamicInterval(task);

            assert !(future.isCancelled());
            assert !(future.isDone());

            Thread.sleep(3000);
            assert !(future.isCancelled());
            assert !(future.isDone());

            future.cancel(true);
            assert future.isCancelled();
            assert future.isDone();
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testIsDone2(TimeScheduler timer) throws InterruptedException {
        try {
            TimeScheduler.Task task=new DynamicTask(new long[]{1000,2000,-1});
            Future<?> future=timer.scheduleWithDynamicInterval(task);

            assert !future.isCancelled();
            assert !future.isDone();

            Thread.sleep(3500);
            assert !future.isCancelled();
            assert future.isDone();

            assert !future.cancel(true); // cancel() fails because the task is done
            assert future.isCancelled();
            assert future.isDone();
            assert !future.cancel(true);
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testIsDone3(TimeScheduler timer) throws InterruptedException {
        try {
            TimeScheduler.Task task=new DynamicTask(new long[]{-1});
            Future<?> future=timer.scheduleWithDynamicInterval(task);
            Thread.sleep(100);
            assert !future.isCancelled();
            assert future.isDone();

            assert !future.cancel(true);
            assert future.isCancelled();
            assert future.isDone();
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void testImmediateExecution(TimeScheduler timer) throws InterruptedException {
        try {
            Promise<Boolean> p=new Promise<Boolean>();
            ImmediateTask task=new ImmediateTask(p);
            timer.execute(task);
            try {
                long start=System.currentTimeMillis(), stop;
                p.getResultWithTimeout(500);
                stop=System.currentTimeMillis();
                System.out.println("task took " + (stop-start) + "ms");
            }
            catch(TimeoutException e) {
                assert false : "ran into timeout - task should have executed immediately";
            }
        }
        finally {
            timer.stop();
        }
    }


    @Test(dataProvider="createTimer")
    public void test2Tasks(TimeScheduler timer) throws InterruptedException {
        int size;
        try {
            System.out.println(System.currentTimeMillis() + ": adding task");
            timer.schedule(new MyTask(), 500, TimeUnit.MILLISECONDS);
            size=timer.size();
            System.out.println("queue size=" + size);
            Assert.assertEquals(1, size);
            Thread.sleep(1000);
            size=timer.size();
            System.out.println("queue size=" + size);
            Assert.assertEquals(0, size);

            Thread.sleep(1500);
            System.out.println(System.currentTimeMillis() + ": adding task");
            timer.schedule(new MyTask(), 500, TimeUnit.MILLISECONDS);

            System.out.println(System.currentTimeMillis() + ": adding task");
            timer.schedule(new MyTask(), 500, TimeUnit.MILLISECONDS);

            System.out.println(System.currentTimeMillis() + ": adding task");
            timer.schedule(new MyTask(), 500, TimeUnit.MILLISECONDS);

            size=timer.size();
            System.out.println("queue size=" + size);
            Assert.assertEquals(3, size);

            Thread.sleep(1000);
            size=timer.size();
            System.out.println("queue size=" + size);
            Assert.assertEquals(0, size);
        }
        finally {
            timer.stop();
        }
    }





    /**
     * Tests whether retransmits are called at correct times for 1000 messages. A retransmit should not be
     * more than 30% earlier or later than the scheduled retransmission time
     */
    @Test(dataProvider="createTimer")
    public void testRetransmits(TimeScheduler timer) throws InterruptedException {
        Entry entry;
        int num_non_correct_entries=0;
        Map<Long,Entry> msgs=new ConcurrentHashMap<Long,Entry>(); // keys=seqnos (Long), values=Entries

        try {
            // 1. Add NUM_MSGS messages:
            System.out.println("-- adding " + NUM_MSGS + " messages:");
            for(long i=0; i < NUM_MSGS; i++) {
                entry=new Entry(i);
                msgs.put(new Long(i), entry);
                timer.scheduleWithDynamicInterval(entry);
            }
            System.out.println("-- done");

            // 2. Wait for at least 4 xmits/msg: total of 1000 + 2000 + 4000 + 8000ms = 15000ms; wait for 20000ms
            System.out.println("-- waiting for all retransmits");

            // 3. Check whether all Entries have correct retransmission times
            long end_time=System.currentTimeMillis() + 20000L, start=System.currentTimeMillis();
            while(System.currentTimeMillis() < end_time) {
                num_non_correct_entries=check(msgs, false);
                if(num_non_correct_entries == 0)
                    break;
                Util.sleep(1000);
            }
            System.out.println("-- waited for " + (System.currentTimeMillis() - start) + " ms");

            num_non_correct_entries=check(msgs, true);
            if(num_non_correct_entries > 0)
                System.err.println("Number of incorrect retransmission timeouts: " + num_non_correct_entries);
            assert num_non_correct_entries == 0: "expected 0 incorrect entries but got " + num_non_correct_entries;
        }
        finally {
            timer.stop();
        }
    }


    /**
     * Tests adding a task and - before it executes - adding tasks which are to be executed sooner
     * @param timer
     */
    @Test(dataProvider="createTimer")
    public void testTasksPreemptingEachOther(TimeScheduler timer) {
        final List<Integer> results=new ArrayList<Integer>(3);

        long execution_time=4000;
        final long base=System.currentTimeMillis();

        for(int num: new Integer[]{1,2,3}) {
            final int cnt=num;
            timer.schedule(new Runnable() {
                public void run() {
                    results.add(cnt);
                    System.out.println("[" + (System.currentTimeMillis() - base) + "] " + cnt);
                }
            }, execution_time, TimeUnit.MILLISECONDS);
            execution_time-=1300;
            Util.sleep(300);
        }

        for(int i=0; i < 10; i++) {
            if(results.size() == 3)
                break;
            Util.sleep(500);
        }

        System.out.println("results = " + results);
        assert results.size() == 3: "results = " + results;
        assert results.get(0) == 3: "results = " + results;
        assert results.get(1) == 2: "results = " + results;
        assert results.get(2) == 1: "results = " + results;
    }


    /**
     * Tests the initial-delay argument of
     * {@link TimeScheduler#scheduleWithFixedDelay(Runnable, long, long, java.util.concurrent.TimeUnit)}
     */
    @Test(dataProvider="createTimer")
    public void testSchedulerWithFixedDelay(TimeScheduler timer) {
        final AtomicBoolean set=new AtomicBoolean(false);

        Future<?> future=timer.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                set.set(true);
            }
        }, 0, 3000, TimeUnit.MILLISECONDS);

        Util.sleep(500);
        future.cancel(true);

        System.out.println("variable was set: " + set);
        assert set.get();

        future=timer.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                set.set(true);
            }
        }, 300, 3000, TimeUnit.MILLISECONDS);

        Util.sleep(1000);
        future.cancel(true);

        System.out.println("variable was set: " + set);
        assert set.get();
    }



    static int check(Map<Long,Entry> msgs, boolean print) {
        int retval=0;
        for(long i=0; i < NUM_MSGS; i++) {
            Entry entry=msgs.get(new Long(i));
            if(!entry.isCorrect(print)) {
                retval++;
            }
        }
        return retval;
    }


    static private class ImmediateTask implements Runnable {
        Promise<Boolean> p;

        public ImmediateTask(Promise<Boolean> p) {
            this.p=p;
        }

        public void run() {
            p.setResult(true);
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
        List<Long> execution_times=new LinkedList<Long>();
        private long base=0;

        RepeatingTask(long timeout) {
            super(timeout);
        }

        public int getNum() {
            return num;
        }

        public List<Long> getExecutionTimes() {
            return execution_times;
        }

        public void run() {
            if(base == 0)
                base=System.currentTimeMillis();
            long time=System.currentTimeMillis() - base;
            System.out.println((num +1) + ": this is a repeating task (" + time + "ms after start)");
            execution_times.add(time);
            num++;
        }
    }


    static class StressTask implements TimeScheduler.Task {
        int num_executions=0;

        public int getNumExecutions() {
            return num_executions;
        }

        public long nextInterval() {
            return 50;
        }

        public void run() {
            num_executions++;
        }
    }


    static class MyRunnable implements Runnable {
        final int num;

        public MyRunnable(int num) {
            this.num=num;
        }

        public void run() {
            System.out.println("[" + num + "] at " + System.currentTimeMillis());
        }
    }


    private static class Entry implements TimeScheduler.Task {
        long start_time=0;  // time message was added
        long first_xmit=0;  // time between start_time and first_xmit should be ca. 1000ms
        long second_xmit=0; // time between first_xmit and second_xmit should be ca. 2000ms
        long third_xmit=0;  // time between third_xmit and second_xmit should be ca. 4000ms
        long fourth_xmit=0; // time between third_xmit and second_xmit should be ca. 8000ms
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
        boolean isCorrect(boolean print) {
            long t;
            long expected;
            long diff, delta;

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
            }
            return false;
        }

        public String toString() {
            StringBuilder sb=new StringBuilder();
            sb.append(first_xmit - start_time).append(", ").append(second_xmit - first_xmit).append(", ");
            sb.append(third_xmit - second_xmit).append(", ").append(fourth_xmit - third_xmit);
            return sb.toString();
        }
    }



}
