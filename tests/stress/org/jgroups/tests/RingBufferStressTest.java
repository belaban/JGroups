package org.jgroups.tests;

import org.jgroups.util.RingBuffer;
import org.jgroups.util.Util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.LinkedList;
import java.util.Collections;


/**
 * Tests {@link org.jgroups.util.RingBuffer} for concurrent insertion and removal
 * @author Bela Ban
 * @version $Id: RingBufferStressTest.java,v 1.4 2010/02/17 16:50:58 belaban Exp $
 */
public class RingBufferStressTest {

    static void start(int capacity, int num_msgs, int num_adders, int num_removers) {
        final RingBuffer<Integer> buffer=new RingBuffer<Integer>(capacity);
        final AtomicInteger added=new AtomicInteger(0);
        final AtomicInteger seqno=new AtomicInteger(1);
        final AtomicInteger removed=new AtomicInteger(0);

        final Adder[] adders=new Adder[num_adders];
        final Remover[] removers=new Remover[num_removers];


        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("added=" + added + ", removed=" + removed + ", seqno=" + seqno);
                dump(removers);
            }
        });

        final CountDownLatch latch=new CountDownLatch(1);


        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(buffer, latch, num_msgs, added, seqno);
            adders[i].start();
        }
        
        for(int i=0; i < removers.length; i++) {
            removers[i]=new Remover(buffer, latch, num_msgs, removed);
            removers[i].start();
        }


        long start=System.currentTimeMillis();
        latch.countDown(); // starts all adders and removers


        /*while(true) {
            if(Util.keyPress("<enter> to remove") == 'x')
                break;
            buffer.remove();
        }*/

        for(Adder adder: adders) {
            try {
                adder.join();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        final List<Integer> all_values=new LinkedList<Integer>();


        for(Remover remover: removers) {
            try {
                remover.join();
                all_values.addAll(remover.getList());
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }

        long time=System.currentTimeMillis() - start;
        double requests_sec=num_msgs / (time / 1000.0);
        System.out.println("\nTime: " + time + " ms, " + Util.format(requests_sec) + " requests / sec\n");
        System.out.println("Total removed messages: " + removed);

        if(all_values.size() < 100) {
            Collections.sort(all_values);
            System.out.println("values (expected=" + num_msgs + ", removed=" + all_values.size() +
                    "):\n" + Util.print(all_values));
        }

        // dump(all_values);
    }

    static void dump(Remover[] removers) {
        final LinkedList<Integer> list=new LinkedList<Integer>();
        for(Remover remover: removers) {
            list.addAll(remover.getList());
        }

        Collections.sort(list);
        System.out.println("\n" + list.size() + " elements: " + list.getFirst() + " - " + list.getLast());

        System.out.println("Checking for missing elements:");

        int prev=0;
        int count=0;
        for(Integer i: list) {
            if(prev +1 != i) {
                System.err.println((prev+1) + " is missing, sequence: prev=" + (prev) + ", current=" + i);
                count+=i - prev -1;
                prev=i;
            }
            else
                prev++;
        }

        if(count == 0)
            System.out.println("Found no missing elements\n");
        else
            System.out.println("Found " + count + " missing elements\n");

    }


    static class Adder extends Thread {
        final RingBuffer<Integer> buffer;
        final CountDownLatch              latch;
        final AtomicInteger               added_msgs;
        final AtomicInteger               current_seqno;
        final int num_msgs;


        public Adder(RingBuffer<Integer> buffer, CountDownLatch latch, int num_msgs, AtomicInteger added_msgs,
                     AtomicInteger current_seqno) {
            this.buffer=buffer;
            this.latch=latch;
            this.added_msgs=added_msgs;
            this.current_seqno=current_seqno;
            this.num_msgs=num_msgs;
            setName("Adder");
        }


        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return;
            }

            while(true) {
                if(added_msgs.incrementAndGet() > num_msgs) {
                    added_msgs.decrementAndGet();
                    break;
                }
                int seqno=current_seqno.getAndIncrement();
                buffer.add(seqno);
            }
        }
    }

    static class Remover extends Thread {
        final RingBuffer<Integer>         buffer;
        final CountDownLatch              latch;
        final int                         msgs_to_remove;
        final AtomicInteger               removed_msgs;
        final LinkedList<Integer>         list=new LinkedList<Integer>();


        public Remover(RingBuffer<Integer> buffer, CountDownLatch latch, int msgs_to_remove, AtomicInteger removed_msgs) {
            this.buffer=buffer;
            this.latch=latch;
            this.msgs_to_remove=msgs_to_remove;
            this.removed_msgs=removed_msgs;
            setName("Remover");
        }


        public List<Integer> getList() {
            return list;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
                return;
            }

            while(removed_msgs.get() < msgs_to_remove) {
                Integer obj=buffer.remove();
                if(obj != null) {
                    removed_msgs.incrementAndGet();
                    list.add(obj);
                }
            }
        }

    }


    public static void main(String[] args) {
        int capacity=100;
        int num_adders=10;
        int num_removers=1;
        int num_msgs=1000000;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-num_adders")) {
                num_adders=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_removers")) {
                num_removers=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-num_msgs")) {
                num_msgs=Integer.parseInt(args[++i]);
                continue;
            }
            if(args[i].equals("-capacity")) {
                capacity=Integer.parseInt(args[++i]);
                continue;
            }
            System.out.println("BlockingRingBufferStressTest [-capacity <buffer capacity>] [-num_msgs msgs] " +
                    "[-num_adders adders] [-num_removers removers]");
            return;
        }
        start(capacity, num_msgs, num_adders, num_removers);
    }
}