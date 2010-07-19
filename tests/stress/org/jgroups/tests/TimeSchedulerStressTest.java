package org.jgroups.tests;

import org.jgroups.util.DefaultTimeScheduler;
import org.jgroups.util.Promise;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.Util;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;

/**
 * @author Bela Ban
 * @version $Id: TimeSchedulerStressTest.java,v 1.2 2010/07/19 11:29:01 belaban Exp $
 */
public class TimeSchedulerStressTest {
    final TimeScheduler timer;
    final int num_threads;
    final int num_tasks; // to process per thread
    final long task_duration; // ms
    final long interval; // ms between tasks

    final CyclicBarrier barrier;


    public TimeSchedulerStressTest(TimeScheduler timer, int num_threads, int num_tasks, long task_duration, long interval) {
        this.timer=timer;
        this.num_threads=num_threads;
        this.task_duration=task_duration;
        this.interval=interval;
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

        for(MyThread thread: threads)
            thread.join();
        long diff=System.currentTimeMillis() - start;
        System.out.println("Time: " + diff + " ms");

        timer.stop();
    }


    class MyThread extends Thread {

        public void run() {
            try {
                barrier.await();
            }
            catch(Exception e) {
            }
            for(int i=0; i < num_tasks; i++) {
                MyTask task=new MyTask();
                timer.schedule(task, task_duration, TimeUnit.MILLISECONDS);
                task.get();
                Util.sleep(interval);
            }
        }
    }


    static class MyTask implements Runnable {
        final Promise<Boolean> result=new Promise<Boolean>();

        public void run() {
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
        long interval=0; //
        TimeScheduler timer=null;

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
            if(args[i].equals("-interval")) {
                interval=Long.parseLong(args[++i]);
                continue;
            }
            if(args[i].equals("-type")) {
                String tmp=args[++i];
                if(tmp.equals("default")) {
                    timer=new DefaultTimeScheduler(num_threads); // ? That's not what we do in real life !
                }
                else {
                    help();
                    return;
                }
                continue;
            }

            help();
            return;
        }

        if(timer == null) {
            System.out.println("timer is null, using DefaultTimeScheduler with " + num_threads + " threads");
            timer=new DefaultTimeScheduler(num_threads);
        }

        TimeSchedulerStressTest test=new TimeSchedulerStressTest(timer, num_threads, num_tasks, task_duration, interval);
        test.start();
    }

    static void help() {
        System.out.println("TimeSchedulerStressTest [-type <\"default\">] [-num_threads <num>] [-num_tasks <num>]" +
                " [-task_duration <ms>] [-interval <ms>]");
    }
}
