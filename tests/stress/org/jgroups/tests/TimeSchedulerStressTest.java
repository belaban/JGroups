package org.jgroups.tests;

import org.jgroups.util.*;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Bela Ban
 */
public class TimeSchedulerStressTest {
    final TimeScheduler timer;
    final int num_threads;
    final int num_tasks;       // to process per thread
    final long task_duration;  // ms

    final CyclicBarrier barrier;
    final AtomicInteger total_tasks=new AtomicInteger(0);
    final AtomicInteger total_sched=new AtomicInteger(0);
    final AtomicInteger total_task_invocations=new AtomicInteger(0);

    static final int NUM_THREADS_IN_TIMER=5;


    public TimeSchedulerStressTest(TimeScheduler timer, int num_threads, int num_tasks, long task_duration) {
        this.timer=timer;
        this.num_threads=num_threads;
        this.task_duration=task_duration;
        this.num_tasks=num_tasks;
        barrier=new CyclicBarrier(num_threads +1);
    }


    public void start() throws Exception {
        MyThread[] threads=new MyThread[num_threads];
        for(int i=0; i < threads.length; i++) {
            threads[i]=new MyThread();
            threads[i].setName("thread-" + i);
            threads[i].start();
        }

        Util.sleep(1000);
        System.out.println("starting " + threads.length + " threads");
        long start=System.currentTimeMillis();
        barrier.await();

        Reporter reporter=new Reporter();
        reporter.setDaemon(true);
        reporter.start();

        for(MyThread thread: threads)
            thread.join();
        long diff=System.currentTimeMillis() - start;
        timer.stop();
        
        System.out.println("Time: " + diff + " ms for " + total_tasks + " tasks");
        System.out.println("running tasks: " + timer.size() + ", total_sched: " + total_sched + ", completed: " + total_tasks +
        ", total task invocations: " + total_task_invocations);
    }


    class MyThread extends Thread {

        public void run() {
            try {
                barrier.await();
            }
            catch(Exception e) {
                e.printStackTrace();
            }

            MyTask[] tasks=new MyTask[num_tasks];

            for(int i=0; i < num_tasks; i++) {
                tasks[i]=new MyTask();
                timer.schedule(tasks[i], task_duration, TimeUnit.MILLISECONDS);
                total_sched.incrementAndGet();
            }

            for(MyTask task: tasks) {
                task.get();
                total_tasks.incrementAndGet();
            }
        }
    }

    class Reporter extends Thread {

        public void run() {
            while(!timer.isShutdown()) {
                System.out.println("running tasks: " + timer.size() + ", total_sched: " + total_sched + ", completed: " + total_tasks +
                        ", total task invocations: " + total_task_invocations);
                Util.sleep(2000);
            }
        }
    }


    class MyTask implements Runnable {
        final Promise<Boolean> result=new Promise<>();

        public void run() {
            total_task_invocations.incrementAndGet();
            result.setResult(true);
        }

        Boolean get() {
            return result.getResult();
        }
    }


    public static void main(String[] args) throws Exception {
        int num_threads=100;
        int num_tasks=100; // to process per thread
        long task_duration=50; // ms
        TimeScheduler timer=timer=new TimeScheduler3();


        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num_threads")) {
                num_threads=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_tasks")) {
                num_tasks=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-task_duration")) {
                task_duration=Long.parseLong(args[++i]);
                continue;
            }
            help();
            return;
        }

        TimeSchedulerStressTest test=new TimeSchedulerStressTest(timer, num_threads, num_tasks, task_duration);
        test.start();
    }

    static void help() {
        System.out.println("TimeSchedulerStressTest [-num_threads <num>] [-num_tasks <num>] [-task_duration <ms>]");
    }
}
