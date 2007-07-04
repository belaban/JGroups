// $Id: ReusableThreadTest.java,v 1.1 2007/07/04 07:29:34 belaban Exp $

package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.util.ReusableThread;
import org.jgroups.util.Util;


public class ReusableThreadTest extends TestCase {


    public ReusableThreadTest(String name) {
        super(name);
    }


    static class MyThread implements Runnable {
        int num=0;
        String name=null;

        public MyThread(int num) {
            this.num=num;
            this.name="Thread #" + num;
        }

        public void run() {

            System.out.println("Thread " + name + " is run");

            long sleep_time=(long)(Math.random() * 5000);
            System.out.print("Thread #" + num + ": sleeping " + sleep_time + ':');
            Util.sleep(sleep_time);
            //throw new Error("hello world");
            System.out.println(" -- done");
        }
    }


    static class LongRunningThread extends MyThread {
        long sleep_time=10000;


        public LongRunningThread(int num) {
            super(num);
            name="LongRunningThread #" + num;
        }

        public LongRunningThread(int num, long sleep_time) {
            super(num);
            this.sleep_time=sleep_time;
            name="LongRunningThread #" + num;
        }

        public void run() {
            System.out.println("LongRunningThread " + name + " is run");
            System.out.println("LongRunningThread #" + num + ": sleeping " + sleep_time + " msecs");
            Util.sleep(sleep_time);
        }
    }




    public void testReusableThread() {
        ReusableThread t=new ReusableThread("Demo ReusableThread");
        MyThread m1=new MyThread(1);
        MyThread m2=new MyThread(2);

        LongRunningThread m4=new LongRunningThread(4);

        System.out.println("Assigning task");
        t.assignTask(m4);

        System.out.println("Sleeping 2 secs");
        Util.sleep(2000);

        System.out.println("stop()");
        t.stop();
        System.out.println("stop() -- done");

        Util.printThreads();

        System.out.println("\nAssigning task 1");
        t.assignTask(m1);
        t.waitUntilDone();  // passive wait
        System.out.println("done with task 1");

        Util.printThreads();

        System.out.println("\nAssigning task 2");
        t.assignTask(m2);
        t.waitUntilDone();
        System.out.println("done with task 2");

        System.out.println("Stopping thread");
        t.stop();
        System.out.println("done");

        Util.printThreads();
    }


    public void testAssignMultipleTimes() {
        ReusableThread t=new ReusableThread("Demo ReusableThread");

        LongRunningThread t1, t2;
        t1=new LongRunningThread(1, 500);
        t2=new LongRunningThread(2, 300);

        t.start();

        t.assignTask(t1);
        t.waitUntilDone();
        assertTrue(t.done());
        t.assignTask(t2);
        t.waitUntilDone();
        assertTrue(t.done());
    }

    public void testStop() {
        ReusableThread t=new ReusableThread("Demo ReusableThread");

        LongRunningThread t1;
        t1=new LongRunningThread(1, 20000);

        t.assignTask(t1);
        Util.sleep(1000);
        t.stop();
        t.waitUntilDone();
        assertTrue(t.done());
        assertFalse(t.isAlive());
    }


    public static void main(String[] args) {
        String[] testCaseName={ReusableThreadTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
