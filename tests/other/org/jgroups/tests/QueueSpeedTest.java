// $Id: QueueSpeedTest.java,v 1.7 2005/05/30 16:15:12 belaban Exp $


package org.jgroups.tests;

import org.jgroups.util.LinkedListQueue;
import org.jgroups.util.Queue;


/**
 * Measures the speed of inserting and removing 1 million elements into/from a Queue and a LinkedQueue.
 *
 * @author Bela Ban
 */
public class QueueSpeedTest {
    int num_elements=1000000;
    final int NUM=10;


    public QueueSpeedTest(int num_elements) {
        this.num_elements=num_elements;
    }


    public void start() throws Exception {
        double q1=0, q2=0, diff;


        System.out.println("warming up cache");
        measureQueue();
        measureLinkedListQueue();

        System.out.println("running insertions " + NUM + " times (will take average)");
        for(int i=0; i < NUM; i++) {
            System.out.println("Round #" + (i + 1));
            q1+=measureQueue();
            q2+=measureLinkedListQueue();
        }

        q1=q1 / NUM;
        q2=q2 / NUM;

        System.out.println("Time to insert and remove " + num_elements + " into Queue:           " + q1 + " ms");
        System.out.println("Time to insert and remove " + num_elements + " into LinkedListQueue: " + q2 + " ms");

        diff=(long)(q2 - q1);
        System.out.println("diff is " + Math.abs(diff) + "; " + (q2 < q1 ? "LinkedListQueue" : "Queue") + " is faster");
    }


    long measureQueue() throws Exception {
        Queue q=new Queue();
        long start, stop;

        start=System.currentTimeMillis();
        for(int i=0; i < num_elements; i++) {
            if(i % 2 == 0)
                q.add(new Integer(i));
            else
                q.addAtHead(new Integer(i));
        }

        while(q.size() > 0)
            q.remove();

        stop=System.currentTimeMillis();
        return stop - start;
    }


    long measureLinkedListQueue() throws Exception {
        LinkedListQueue q=new LinkedListQueue();
        long start, stop;

        start=System.currentTimeMillis();
        for(int i=0; i < num_elements; i++) {
            if(i % 2 == 0)
                q.add(new Integer(i));
            else
                q.addAtHead(new Integer(i));
        }

        while(q.size() > 0)
            q.remove();

        stop=System.currentTimeMillis();
        return stop - start;
    }


    public static void main(String[] args) {
        int num_elements=1000000;

        for(int i=0; i < args.length; i++) {
            if("-num_elements".equals(args[i])) {
                num_elements=Integer.parseInt(args[++i]);
                continue;
            }
            help();
            return;
        }

        try {
            new QueueSpeedTest(num_elements).start();
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
    }


    static void help() {
        System.out.println("QueueSpeedTest [-help] [-num_elements <num>]");
    }
}

