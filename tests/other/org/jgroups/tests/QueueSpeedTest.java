// $Id: QueueSpeedTest.java,v 1.8 2007/07/27 10:49:30 belaban Exp $


package org.jgroups.tests;

import org.jgroups.util.Queue;


/**
 * Measures the speed of inserting and removing 1 million elements into/from a Queue
 *
 * @author Bela Ban
 */
public class QueueSpeedTest {
    int num_elements=1000000;
    static final int NUM=10;


    public QueueSpeedTest(int num_elements) {
        this.num_elements=num_elements;
    }


    public void start() throws Exception {
        double q1=0;


        System.out.println("warming up cache");
        measureQueue();

        System.out.println("running insertions " + NUM + " times (will take average)");
        for(int i=0; i < NUM; i++) {
            System.out.println("Round #" + (i + 1));
            q1+=measureQueue();
        }

        q1=q1 / NUM;

        System.out.println("Time to insert and remove " + num_elements + " into Queue:           " + q1 + " ms");
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

