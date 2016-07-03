
package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;


/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class QueueTest {



    public static void testQueue() throws QueueClosedException {
        final Queue queue=new Queue();
        queue.add("Q1");
        queue.add("Q2");
        queue.add("Q3");

        assert queue.peek().equals("Q1");
        assert queue.remove().equals("Q1");

        assert queue.peek().equals("Q2");
        assert queue.remove().equals("Q2");
        queue.add("Q5");

        queue.close(true);

        try {
            queue.add("Q6");
            assert false : "should not get here";
        }
        catch(org.jgroups.util.QueueClosedException qc) {
        }

        int size=queue.size();
        queue.removeElement("Q5");
        assert queue.size() == size -1;
        assert queue.peek().equals("Q3");
        assert queue.remove().equals("Q3");
        assert queue.closed();
    }


    @Test(expectedExceptions=QueueClosedException.class)
    public static void testCloseWithoutFlush() throws QueueClosedException {
        final Queue queue=new Queue();
        queue.close(false);
        queue.remove();
    }


    @Test(expectedExceptions=QueueClosedException.class)
    public static void testCloseWithFlush() throws QueueClosedException {
        final Queue queue=new Queue();
        queue.close(true);
        queue.remove();
    }


    @Test(expectedExceptions=QueueClosedException.class)
    public static void testCloseWithFlush2() throws QueueClosedException {
        final Queue queue=new Queue();
        queue.add(1);
        queue.add(2);
        queue.add(3);
        queue.close(true);
        for(int i=1; i <= 3; i++) {
            Object obj=queue.remove();
            assert obj != null;
            assert new Integer(i).equals(obj);
        }
        queue.remove();
    }



    public static void testValues() throws QueueClosedException {
        final Queue queue=new Queue();
        queue.add(1);
        queue.add(3);
        queue.add(99);
        queue.add(8);
        System.out.println("queue: " + Util.dumpQueue(queue));
        int size=queue.size();
        assert size == 4;
        LinkedList values=queue.values();
        assert values.size() == size;
    }



    public static void testLargeInsertion() throws QueueClosedException {
        String element="MyElement";
        long start, stop;
        final Queue queue=new Queue();

        System.out.println("Inserting 100000 elements");
        start=System.currentTimeMillis();
        for(int i=0; i < 100000; i++)
            queue.add(element);
        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop - start) + " msecs");

        System.out.println("Removing 100000 elements");
        start=System.currentTimeMillis();
        while(queue.size() > 0)
            queue.remove();
        stop=System.currentTimeMillis();
        System.out.println("Took " + (stop - start) + " msecs");
    }



    public static void testEmptyQueue() {
        final Queue queue=new Queue();
        assert queue.getFirst() == null;
        assert queue.getLast() == null;
    }


    public static void testAddAll() throws QueueClosedException {
        final Queue queue=new Queue();
        List<String> l=new ArrayList<>();
        l.add("one");
        l.add("two");
        l.add("three");
        queue.addAll(l);
        System.out.println("queue is " + queue);
        assert queue.size() == 3;
        assert queue.remove().equals("one");
        assert queue.size() == 2;
        assert queue.remove().equals("two");
        assert queue.size() == 1;
        assert queue.remove().equals("three");
        assert queue.size() == 0;
    }


    public static void testInsertionAndRemoval() throws Exception {
        final Queue queue=new Queue();
        String s1="Q1", s2="Q2";

        queue.add(s1);
        assert queue.getFirst() != null;
        assert queue.getLast() != null;
        assert queue.getLast().equals(queue.getFirst());

        queue.add(s2);
        assert queue.getFirst() != queue.getLast();

        Object o1=queue.peek();
        Object o2=queue.getFirst();

        System.out.println("o1=" + o1 + ", o2=" + o2 + ", o1.equals(o2)=" + o1.equals(o2));

        assert queue.getFirst().equals(queue.peek());
        queue.remove();

        assert queue.size() == 1;
        assert queue.getLast().equals(queue.getFirst());
        queue.remove();

        assert queue.size() == 0;
        assert queue.getFirst() == null;
        assert queue.getLast() == null;
    }



    public static void testWaitUntilClosed() {
        final Queue queue=new Queue();
        queue.close(true);
        queue.waitUntilClosed(0);
        assert queue.size() == 0;
    }


    public static void testWaitUntilClosed2() {
        final Queue queue=new Queue();
        queue.close(true);
        try {
            queue.peek();
            assert false : "peek() should throw a QueueClosedException";
        }
        catch(QueueClosedException e) {
            assert e != null;
        }
        assert queue.size() == 0;
    }


    public static void testWaitUntilClosed3() throws QueueClosedException {
        final Queue queue=new Queue();
        queue.add("one");
        queue.close(true);
        Object obj=queue.peek();
        assert obj.equals("one");
        assert queue.size() == 1;
        queue.remove();
        try {
            queue.peek();
            assert false : "peek() should throw a QueueClosedException";
        }
        catch(QueueClosedException e) {
            assert e != null;
        }
        assert queue.size() == 0;
    }


    public static void testWaitUntilClosed4() throws QueueClosedException {
        final Queue queue=new Queue();
        for(int i=0; i < 10; i++)
            queue.add(i);
        new Thread() {
            public void run() {
                while(!queue.closed()) {
                    try {
                        System.out.println("-- removed " + queue.remove());
                        Util.sleep(200);
                    }
                    catch(QueueClosedException e) {
                        break;
                    }
                }
            }
        }.start();
        queue.close(true);
        queue.waitUntilClosed(0);
        assert queue.size() == 0;
    }



    public static void testWaitUntilClosed5() throws QueueClosedException {
        final Queue queue=new Queue();
        for(int i=0; i < 10; i++)
            queue.add(i);
        new Thread() {
            public void run() {
                while(!queue.closed()) {
                    try {
                        System.out.println("-- removed " + queue.remove());
                        Util.sleep(200);
                    }
                    catch(QueueClosedException e) {
                        System.out.println("-- queue is closed, cannot remove element");
                        break;
                    }
                }
            }
        }.start();

        Util.sleep(600);
        queue.close(false);
        queue.waitUntilClosed(0);
        assert queue.size() > 0;
    }




    public static void testRemoveElementNoElement() {
        final Queue queue=new Queue();
        String s1="Q1";

        try {
            queue.removeElement(s1);
            assert !(queue.closed());
            assert queue.size() == 0;
        }
        catch(QueueClosedException ex) {
            assert false : ex.toString();
        }
    }



    public static void testRemoveElementOneElement() {
        final Queue queue=new Queue();
        String s1="Q1";

        try {
            queue.add(s1);
            queue.removeElement(s1);
            assert queue.size() == 0;
            assert queue.getFirst() == null;
            assert queue.getLast() == null;
        }
        catch(QueueClosedException ex) {
            assert false : ex.toString();
        }
    }


    public static void testRemoveElementTwoElementsFirstFound() {
        String s1="Q1", s2="Q2";
        final Queue queue=new Queue();

        try {
            queue.add(s1);
            queue.add(s2);
            queue.removeElement(s1);
            assert queue.size() == 1;
            assert queue.getFirst().equals(s2);
            assert queue.getLast().equals(s2);
            assert queue.getFirst().equals(queue.getLast());
        }
        catch(QueueClosedException ex) {
            assert false : ex.toString();
        }
    }


    public static void testRemoveElementTwoElementsSecondFound() {
        String s1="Q1", s2="Q2";
        final Queue queue=new Queue();

        try {
            queue.add(s1);
            queue.add(s2);
            queue.removeElement(s2);
            assert queue.size() == 1;
            assert queue.getFirst().equals(s1);
            assert queue.getLast().equals(s1);
            assert queue.getFirst().equals(queue.getLast());
        }
        catch(QueueClosedException ex) {
            assert false : ex.toString();
        }
    }


    public static void testRemoveElementThreeElementsFirstFound() {
        String s1="Q1", s2="Q2", s3="Q3";
        final Queue queue=new Queue();

        try {
            queue.add(s1);
            queue.add(s2);
            queue.add(s3);
            queue.removeElement(s1);
            assert queue.size() == 2;
            assert queue.getFirst().equals(s2);
            assert queue.getLast().equals(s3);
        }
        catch(QueueClosedException ex) {
            assert false : ex.toString();
        }
    }


    public static void testRemoveElementThreeElementsSecondFound() {
        String s1="Q1", s2="Q2", s3="Q3";
        final Queue queue=new Queue();

        try {
            queue.add(s1);
            queue.add(s2);
            queue.add(s3);
            queue.removeElement(s2);
            assert queue.size() == 2;
            assert queue.getFirst().equals(s1);
            assert queue.getLast().equals(s3);
        }
        catch(QueueClosedException ex) {
            assert false : ex.toString();
        }
    }


    public static void testRemoveElementThreeElementsThirdFound() {
        String s1="Q1", s2="Q2", s3="Q3";
        final Queue queue=new Queue();

        try {
            queue.add(s1);
            queue.add(s2);
            queue.add(s3);
            queue.removeElement(s3);
            assert queue.size() == 2;
            assert queue.getFirst().equals(s1);
            assert queue.getLast().equals(s2);
        }
        catch(QueueClosedException ex) {
            assert false : ex.toString();
        }
    }


    @Test(expectedExceptions=QueueClosedException.class)
    public static void testRemoveAndClose() throws QueueClosedException {
        final Queue queue=new Queue();
        new Thread() {
            public void run() {
                Util.sleep(1000);
                queue.close(true); // close gracefully
            }
        }.start();
        queue.remove();
    }


    @Test(expectedExceptions=QueueClosedException.class)
    public static void testRemoveAndCloseWithTimeout() throws QueueClosedException, TimeoutException {
        final Queue queue=new Queue();
        new Thread() {
            public void run() {
                Util.sleep(1000);
                queue.close(true); // close gracefully
            }
        }.start();

        queue.remove(5000);
    }


    @Test(expectedExceptions=TimeoutException.class)
    public static void testInterruptAndRemove() throws QueueClosedException, TimeoutException {
        final Queue queue=new Queue();
        Thread.currentThread().interrupt();
        queue.remove(2000);
    }


    @Test(expectedExceptions=QueueClosedException.class)
    public static void testRemoveAndInterrupt() throws QueueClosedException {
        final Queue queue=new Queue();

        Thread closer=new Thread() {
            public void run() {
                Util.sleep(1000);
                System.out.println("-- closing queue");
                queue.close(false);
            }
        };
        closer.start();

        System.out.println("-- removing element");
        queue.remove();
    }


    public static void testClear() throws QueueClosedException {
        Queue queue=new Queue();
        queue.add("one");
        queue.add("two");
        assert queue.size() == 2;
        queue.close(true);
        assert queue.size() == 2;
        queue.clear();
        assert queue.size() == 0;
        queue=new Queue();
        queue.add("one");
        queue.add("two");
        queue.clear();
        assert queue.size() == 0;
        queue.add("one");
        queue.add("two");
        assert queue.size() == 2;
        queue.clear();
        assert queue.size() == 0;
    }


//    public void testWaitUntilEmpty() {
//        try {
//            queue.add("one");
//            queue.add("two");
//            queue.add("three");
//
//            new Thread() {
//                public void run() {
//                    try {
//                        sleep(1000);
//                        queue.remove();
//                        queue.remove();
//                        queue.remove();
//                    }
//                    catch(Exception e) {
//                    }
//                }
//            }.start();
//
//            queue.waitUntilEmpty(0);
//            assertEquals(queue.size(), 0);
//        }
//        catch(Exception e) {
//            e.printStackTrace();
//            fail(e.toString());
//        }
//    }
//
//    public void testWaitUntilEmpty2() {
//        try {
//            queue.add("one");
//            queue.add("two");
//            queue.add("three");
//
//            new Thread() {
//                public void run() {
//                    try {
//                        sleep(1000);
//                        queue.remove();
//                        queue.remove();
//                    }
//                    catch(Exception e) {
//                    }
//                }
//            }.start();
//
//            queue.waitUntilEmpty(3000);
//            fail("shouldn't get here; we should have caught a TimeoutException");
//        }
//        catch(TimeoutException timeout) {
//            assertTrue(true);
//        }
//        catch(Exception e) {
//            e.printStackTrace();
//            fail(e.toString());
//        }
//    }
//
//
//    public void testWaitUntilQueueClosed() {
//         try {
//            queue.add("one");
//            queue.add("two");
//            queue.add("three");
//
//            new Thread() {
//                public void run() {
//                    try {
//                        sleep(1000);
//                        queue.close(false);
//                    }
//                    catch(Exception e) {
//                    }
//                }
//            }.start();
//
//            queue.waitUntilEmpty(0);
//            fail("shouldn't get here; we should have caught a QueueClosedException");
//        }
//        catch(TimeoutException timeout) {
//            fail("we should not have gottem here");
//        }
//         catch(QueueClosedException ex2) {
//             assertTrue(true);
//         }
//        catch(Exception e) {
//             e.printStackTrace();
//             fail();
//        }
//    }


    /** Multiple threads call remove(), one threads then adds an element. Only 1 thread should actually terminate
     * (the one that has the element) */
    public static void testBarrier() throws QueueClosedException {
        RemoveOneItem[] removers=new RemoveOneItem[10];
        final Queue queue=new Queue();
        int num_dead=0;

        for(int i=0; i < removers.length; i++) {
            removers[i]=new RemoveOneItem(i, queue);
            removers[i].start();
        }

        Util.sleep(200);

        System.out.println("-- adding element 99");
        queue.add(99L);
        System.out.println("-- adding element 100");
        queue.add(100L);

        long target_time=System.currentTimeMillis() + 10000L;
        do {
            int num=0;
            for(int i=0; i < removers.length; i++) {
                if(!removers[i].isAlive())
                    num++;
            }
            if(num == 2)
                break;
            Util.sleep(500);
        }
        while(target_time > System.currentTimeMillis());


        for(int i=0; i < removers.length; i++) {
            System.out.println("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
            if(!removers[i].isAlive()) {
                num_dead++;
            }
        }

        assert num_dead == 2 : "num_dead was " + num_dead + ", but expected 2";
        queue.close(false);
    }

    /** Multiple threads call remove(), one threads then adds an element. Only 1 thread should actually terminate
     * (the one that has the element) */
    public static void testBarrierWithTimeOut() throws QueueClosedException {
        final Queue queue=new Queue();
        RemoveOneItemWithTimeout[] removers=new RemoveOneItemWithTimeout[10];
        int num_dead=0;

        for(int i=0; i < removers.length; i++) {
            removers[i]=new RemoveOneItemWithTimeout(i, 15000, queue);
            removers[i].start();
        }

        System.out.println("-- adding element 99");
        queue.add((long)99);
        System.out.println("-- adding element 100");
        queue.add((long)100);

        long target_time=System.currentTimeMillis() + 10000L;
        do {
            int num_rsps=0;
            for(int i=0; i < removers.length; i++) {
                if(removers[i].getRetval() != null)
                    num_rsps++;
            }
            if(num_rsps == 2)
                break;
            Util.sleep(500);
        }
        while(target_time > System.currentTimeMillis());

        Util.sleep(3000);

        for(int i=0; i < removers.length; i++) {
            System.out.println("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
            if(!removers[i].isAlive()) {
                num_dead++;
            }
        }

        assert num_dead == 2 : "num_dead should have been 2 but was " + num_dead;

        System.out.println("closing queue - causing all remaining threads to terminate");
        queue.close(false); // will cause all threads still blocking on remove() to return
        Util.sleep(500);

        num_dead=0;
        for(int i=0; i < removers.length; i++) {
            System.out.println("remover #" + i + " is " + (removers[i].isAlive()? "alive" : "terminated"));
            if(!removers[i].isAlive()) {
                num_dead++;
            }
        }
        assert num_dead == 10 : "num_dead should have been 10 but was " + num_dead;
    }


    /** Multiple threads add one element, one thread read them all.
     * (the one that has the element) */

    public static void testMultipleWriterOneReader() throws QueueClosedException {
        final Queue queue=new Queue();
        AddOneItem[] adders=new AddOneItem[10];
        int num_dead=0;
        int num_items=0;
        int items=1000;

        for(int i=0; i < adders.length; i++) {
            adders[i]=new AddOneItem(i, items, queue);
            adders[i].start();
        }

        Util.sleep(500);
        while(num_items < (adders.length * items)) {
            queue.remove();
            num_items++;
        }

        Util.sleep(1000);

        for(int i=0; i < adders.length; i++) {
            System.out.println("adder #" + i + " is " + (adders[i].isAlive()? "alive" : "terminated"));
            if(!adders[i].isAlive()) {
                num_dead++;
            }
        }

        assert num_dead == 10 : "num_dead should have been 10 but was " + num_dead;
        queue.close(false); // will cause all threads still blocking on peek() to return
    }


    /**
     * Times how long it takes to add and remove 1000000 elements concurrently (1 reader, 1 writer)
     */

    public static void testConcurrentAddRemove() throws QueueClosedException {
        final Queue queue=new Queue();
        final long   NUM=1000000;
        long         num_received=0;
        Object       ret;
        long         start, stop;

        start=System.currentTimeMillis();

        new Thread() {
            public void run() {
                for(int i=0; i < NUM; i++) {
                    try {
                        queue.add(new Object());
                    }
                    catch(QueueClosedException e) {
                    }
                }
            }
        }.start();

        while(num_received < NUM) {
            ret=queue.remove();
            if(ret != null)
                num_received++;
        }
        assert num_received == NUM;
        stop=System.currentTimeMillis();
        System.out.println("time to add/remove " + NUM + " elements: " + (stop-start));
    }



    /** Has multiple threads add(), remove() and peek() elements to/from the queue */

    public static void testConcurrentAccess() {
        final Queue queue=new Queue();
        final int NUM_THREADS=10;
        final int INTERVAL=5000;

        Writer[] writers=new Writer[NUM_THREADS];
        Reader[] readers=new Reader[NUM_THREADS];
        int[] writes=new int[NUM_THREADS];
        int[] reads=new int[NUM_THREADS];
        long total_reads=0, total_writes=0;


        for(int i=0; i < writers.length; i++) {
            readers[i]=new Reader(i, reads, queue);
            readers[i].start();
            writers[i]=new Writer(i, writes, queue);
            writers[i].start();
        }

        Util.sleep(INTERVAL);

        System.out.println("current queue size=" + queue.size());

        for(int i=0; i < writers.length; i++) {
            writers[i].stopThread();
        }

        for(int i=0; i < readers.length; i++) {
            readers[i].stopThread();
        }

        queue.close(false); // will cause all threads still blocking on peek() to return

        System.out.println("current queue size=" + queue.size());

        for(int i=0; i < writers.length; i++) {
            try {
                writers[i].join(300);
                readers[i].join(300);
            }
            catch(Exception ex) {
                System.err.println(ex);
            }
        }


        for(int i=0; i < writes.length; i++) {
            System.out.println("Thread #" + i + ": " + writes[i] + " writes, " + reads[i] + " reads");
            total_writes+=writes[i];
            total_reads+=reads[i];
        }
        System.out.println("total writes=" + total_writes + ", total_reads=" + total_reads +
                ", diff=" + Math.abs(total_writes - total_reads));
    }

    static class AddOneItem extends Thread {
        Long retval=null;
        int rank=0;
        int iteration=0;
        Queue queue;

        AddOneItem(int rank, int iteration, Queue queue) {
            super("AddOneItem thread #" + rank);
            this.rank=rank;
            this.iteration=iteration;
            setDaemon(true);
            this.queue=queue;
        }

        public void run() {
            try {
                for(int i=0; i < iteration; i++) {
                    queue.add((long)rank);
                    // Util.sleepRandom(1);
                    // System.out.println("Thread #" + rank + " added element (" + rank + ")");
                }
            }
            catch(QueueClosedException closed) {
                System.err.println("Thread #" + rank + ": queue was closed");
            }
        }

    }

    static class RemoveOneItem extends Thread {
        Long retval=null;
        int rank=0;
        Queue queue;


        RemoveOneItem(int rank, Queue queue) {
            super("RemoveOneItem thread #" + rank);
            this.rank=rank;
            this.queue=queue;
            setDaemon(true);
        }

        public void run() {
            try {
                retval=(Long)queue.remove();
                // System.out.println("Thread #" + rank + " removed element (" + retval + ")");
            }
            catch(QueueClosedException closed) {
                System.err.println("Thread #" + rank + ": queue was closed");
            }
        }

        Long getRetval() {
            return retval;
        }
    }

    static class RemoveOneItemWithTimeout extends Thread {
        Long retval=null;
        final int rank;
        final long timeout;
        final Queue queue;

        RemoveOneItemWithTimeout(int rank, long timeout, Queue queue) {
            super("RemoveOneItem thread #" + rank);
            this.rank=rank;
            this.timeout=timeout;
            this.queue=queue;
            setDaemon(true);
        }

        public void run() {
            try {
                retval=(Long)queue.removeWait(timeout);
                System.out.println("Thread #" + rank + ": retrieved " + retval);
            }
            catch(QueueClosedException closed) {
                System.out.println("Thread #" + rank + ": queue was closed");
            }
            catch(TimeoutException e) {
                System.out.println("Thread #" + rank + ": timeout occurred");
            }
        }

        Long getRetval() {
            return retval;
        }
    }




    static class Writer extends Thread {
        int rank=0;
        int num_writes=0;
        boolean running=true;
        int[] writes=null;
        Queue queue;

        Writer(int i, int[] writes, Queue queue) {
            super("WriterThread");
            rank=i;
            this.writes=writes;
            this.queue=queue;
            setDaemon(true);
        }


        public void run() {
            while(running) {
                try {
                    queue.add(System.currentTimeMillis());
                    num_writes++;
                }
                catch(QueueClosedException closed) {
                    running=false;
                }
                catch(Throwable t) {
                    System.err.println("QueueTest.Writer.run(): exception=" + t);
                }
            }
            writes[rank]=num_writes;
        }

        void stopThread() {
            running=false;
        }
    }


    static class Reader extends Thread {
        int rank;
        int num_reads=0;
        int[] reads=null;
        boolean running=true;
        Queue queue;


        Reader(int i, int[] reads, Queue queue) {
            super("ReaderThread");
            rank=i;
            this.reads=reads;
            this.queue=queue;
            setDaemon(true);
        }


        public void run() {
            Long el;

            while(running) {
                try {
                    el=(Long)queue.remove();
                    if(el == null) { // @remove
                        System.out.println("QueueTest.Reader.run(): peek() returned null element. " +
                                "queue.size()=" + queue.size() + ", queue.closed()=" + queue.closed());
                    }
                    assert el != null;
                    num_reads++;
                }
                catch(QueueClosedException closed) {
                    running=false;
                }
                catch(Throwable t) {
                    System.err.println("QueueTest.Reader.run(): exception=" + t);
                }
            }
            reads[rank]=num_reads;
        }


        void stopThread() {
            running=false;
        }

    }



}
