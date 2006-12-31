package org.jgroups.tests;

import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 *
 * @author bela
 * Date: Jul 25, 2003
 * Time: 2:14:32 PM
 */
public class QueueTest2 {
    Queueable q=null;



    long  start, stop;
    long  NUM=1000 * 1000;

    void start(Queueable q, String msg) throws Exception {
        this.q=q;
        System.out.println("-- starting test with " + q.getClass() + " (" + msg + ')');
        start=System.currentTimeMillis();
        Adder adder=new Adder();
        Remover remover=new Remover();
        remover.start();
        adder.start();
        adder.join();
        remover.join();
        System.out.println("-- done with " + q.getClass());
        System.out.println(" total time for " + NUM + " elements: " + (stop-start) + " msecs\n\n");
    }


    public interface Queueable {
        void addElement(Object o);
        Object removeElement();
    }

    class Adder extends Thread {
        public void run() {
            for(int i=0; i < NUM; i++) {
                try {
                    q.addElement(new Integer(i));
                    //if(i % 1000 == 0)
                      //  System.out.println("-- added " + i);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            //System.out.println("-- Adder: done");
        }
    }

    class Remover extends Thread {
        int i=0;
        public void run() {
            do {
                try {
                    if(q.removeElement() != null)
                        i++;
                    else {
                        Thread.sleep(1);
                    }
                    //if(i % 1000 == 0)
                      //  System.out.println("-- removed " + i);
                }
                catch(Exception e) {
                    e.printStackTrace();
                }
            }
            while(i < NUM);
            stop=System.currentTimeMillis();
            // System.out.println("-- Remover: done");
        }
    }


    public static class JgQueue extends Queue implements Queueable {

        public void addElement(Object o) {
            try {
                add(o);
            }
            catch(QueueClosedException e) {
                e.printStackTrace();
            }
        }

        public Object removeElement() {
            try {
                return remove();
            }
            catch(QueueClosedException e) {
                e.printStackTrace();
                return null;
            }
        }
    }



    public static class MyQueue extends LinkedList implements Queueable {
        final Object mutex=new Object();
        boolean waiting=false; // remover waiting on mutex

        public void addElement(Object o) {
            synchronized(mutex) {
                super.add(o);
                if(waiting)
                    mutex.notifyAll();
            }
        }


        public Object removeElement() {
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


    public static class MyLinkedQueue implements Queueable {
        ConcurrentLinkedQueue q=new ConcurrentLinkedQueue();
        public void addElement(Object o) {
            q.add(o);
        }

        public Object removeElement() {
            return q.poll();
        }
    }




    public static void main(String[] args) {
        try {
            QueueTest2 qt=new QueueTest2();

            Queueable q=new JgQueue();
            qt.start(q, "based on org.jgroups.util.Queue");

            q=new MyQueue();
            qt.start(q, "based on java.util.LinkedList");

            q=new MyLinkedQueue();
            qt.start(q, "java.util.concurrent.ConcurrentLinkedList");
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

}
