package org.jgroups.tests;

import java.util.LinkedList;

import EDU.oswego.cs.dl.util.concurrent.LinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.WaitFreeQueue;
import org.jgroups.util.Queue;

/**
 *
 * @author bela
 * Date: Jul 25, 2003
 * Time: 2:14:32 PM
 */
public class QueueTest2 {
    Queue q=new Queue();
    // MyQueue q=new MyQueue();
    // MyLinkedQueue q=new MyLinkedQueue();
    // MyWaitFreeQueue q=new MyWaitFreeQueue();

    long  start, stop;
    long  NUM=1000 * 1000;

    void start() throws Exception {
        start=System.currentTimeMillis();
        Adder adder=new Adder();
        Remover remover=new Remover();
        remover.start();
        adder.start();
        adder.join();
        remover.join();
        System.out.println("== total time for " + NUM + " elements: " + (stop-start) + " msecs");
    }


    class Adder extends Thread {
        public void run() {
            for(int i=0; i < NUM; i++) {
                try {
                    q.add(new Integer(i));
                    //if(i % 1000 == 0)
                      //  System.out.println("-- added " + i);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            System.out.println("-- Adder: done");
        }
    }

    class Remover extends Thread {
        int i=0;
        public void run() {
            do {
                try {
                    q.remove();
                    i++;
                    //if(i % 1000 == 0)
                      //  System.out.println("-- removed " + i);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            while(i < NUM);
            stop=System.currentTimeMillis();
            System.out.println("-- Remover: done");
        }
    }


    public static class MyQueue extends LinkedList {
        Object mutex=new Object();
        boolean waiting=false; // remover waiting on mutex

        public boolean add(Object o) {
            synchronized(mutex) {
                boolean retval=super.add(o);
                if(waiting)
                    mutex.notifyAll(); // todo: change to notify()
                return retval;
            }
        }


        public Object remove() {
            synchronized(mutex) {
                if(size() > 0) {
                    return removeFirst();
                }
                else {
                    waiting=true;
                    try {
                        mutex.wait();
                        return removeFirst();
                    }
                    catch(InterruptedException e) {
                        e.printStackTrace();
                        return null;
                    }
                    finally {
                        waiting=false;
                    }
                }
            }
        }
    }


    public static class MyLinkedQueue extends LinkedQueue {
        public void add(Object o) {
            try {
                super.put(o);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        public Object remove() {
            try {
                return super.take();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


      public static class MyWaitFreeQueue extends WaitFreeQueue {
        public void add(Object o) {
            try {
                super.put(o);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        public Object remove() {
            try {
                return super.take();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return null;
            }
        }
    }


    public static void main(String[] args) {
        try {
            new QueueTest2().start();

//            MyQueue q=new MyQueue();
//            q.add("Bela");
//            q.add("Jeannette");
//            System.out.println(q);
//            q.remove();
//            System.out.println(q);

        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

}
