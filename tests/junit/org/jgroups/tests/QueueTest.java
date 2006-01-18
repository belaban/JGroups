// $Id: QueueTest.java,v 1.20 2006/01/18 13:09:44 belaban Exp $

package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.TimeoutException;
import org.jgroups.util.Queue;
import org.jgroups.util.QueueClosedException;
import org.jgroups.util.Util;

import java.util.LinkedList;
import java.util.ArrayList;


public class QueueTest extends TestCase {
    private Queue queue=null;

    public QueueTest(String Name_) {
        super(Name_);
    }

    public void setUp() throws Exception {
        super.setUp();
        queue=new Queue();
    }


    public void tearDown() throws Exception {
        super.tearDown();
        if(queue != null) {
            queue.reset();
        }
    }


    public void testQueue() {
        try {
            queue.add("Q1");
            queue.add("Q2");
            queue.add("Q3");

            assertEquals("Q1", queue.peek());
            assertEquals("Q1", queue.remove());

            assertEquals("Q2", queue.peek());
            assertEquals("Q2", queue.remove());

            queue.addAtHead("Q4");
            queue.add("Q5");
            assertEquals("Q4", queue.peek());
            assertEquals("Q4", queue.remove());

            queue.close(true);

            try {
                queue.add("Q6");
                fail("should not get here");
            }
            catch(org.jgroups.util.QueueClosedException qc) {
                assertTrue(true);
            }

            int size=queue.size();
            queue.removeElement("Q5");
            assertEquals((size - 1), queue.size());

            assertEquals("Q3", queue.peek());
            assertEquals("Q3", queue.remove());
            assertTrue(queue.closed());
            System.out.println("Everything is ok");
        }
        catch(Exception x) {
            System.out.println(x);
            fail();
        }
    }


    public void testCloseWithoutFlush() {
        queue.close(false);
        try {
            queue.remove();
            fail("we should have gotten a QueueClosedException trying to remove an element from a closed queue");
        }
        catch(QueueClosedException e) {
            assertTrue("queue is closed, this is okay", queue.closed());
        }
    }


    public void testCloseWithFlush() {
        queue.close(true);
        try {
            queue.remove();
            fail("we should have gotten a QueueClosedException trying to remove an element from a closed queue");
        }
        catch(QueueClosedException e) {
            assertTrue("queue is closed, this is okay", queue.closed());
        }
    }


    public void testValues() throws QueueClosedException {
        queue.add(new Integer(1));
        queue.add(new Integer(3));
        queue.add(new Integer(99));
        queue.add(new Integer(8));
        System.out.println("queue: " + Util.dumpQueue(queue));
        int size=queue.size();
        assertEquals(4, size);
        LinkedList values=queue.values();
        assertEquals(size, values.size());
    }


    public void testLargeInsertion() {
        String element="MyElement";
        long start, stop;

        try {
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
        catch(Exception ex) {
            System.err.println(ex);
            fail();
        }
    }


    public void testEmptyQueue() {
        assertNull(queue.getFirst());
        assertNull(queue.getLast());
        assertEquals(queue.getFirst(), queue.getLast()); // both are null; they're equal
    }

    public void testAddAll() throws QueueClosedException {
        ArrayList l=new ArrayList();
        l.add("one");
        l.add("two");
        l.add("three");
        queue.addAll(l);
        System.out.println("queue is " + queue);
        assertEquals(3, queue.size());
        assertEquals("one", queue.remove());
        assertEquals(2, queue.size());
        assertEquals("two", queue.remove());
        assertEquals(1, queue.size());
        assertEquals("three", queue.remove());
        assertEquals(0, queue.size());
    }

    public void testInsertionAndRemoval() throws Exception {
        String s1="Q1", s2="Q2";

        queue.add(s1);
        assertTrue(queue.getFirst() != null);
        assertTrue(queue.getLast() != null);
        assertEquals(queue.getFirst(), queue.getLast());

        queue.add(s2);
        assertTrue(queue.getFirst() != queue.getLast());

        Object o1=queue.peek();
        Object o2=queue.getFirst();

        System.out.println("o1=" + o1 + ", o2=" + o2 + ", o1 == o2=" + o1 == o2 + ", o1.equals(o2)=" + o1.equals(o2));

        assertEquals(queue.peek(), queue.getFirst());
        queue.remove();

        assertEquals(1, queue.size());
        assertEquals(queue.getFirst(), queue.getLast());
        queue.remove();

        assertEquals(0, queue.size());
        assertTrue(queue.getFirst() == null);
        assertTrue(queue.getLast() == null);
    }


    public void testWaitUntilClosed() {
        queue.close(true);
        queue.waitUntilClosed(0);
        assertEquals(0, queue.size());
    }

    public void testWaitUntilClosed2() {
        queue.close(true);
        try {
            queue.peek();
            fail("peek() should throw a QueueClosedException");
        }
        catch(QueueClosedException e) {
            assertTrue(e != null);
        }
        assertEquals(0, queue.size());
    }

    public void testWaitUntilClosed3() throws QueueClosedException {
        queue.add("one");
        queue.close(true);
        Object obj=queue.peek();
        assertEquals("one", obj);
        assertEquals(1, queue.size());
        queue.remove();
        try {
            queue.peek();
            fail("peek() should throw a QueueClosedException");
        }
        catch(QueueClosedException e) {
            assertTrue(e != null);
        }
        assertEquals(0, queue.size());
    }

    public void testWaitUntilClosed4() throws QueueClosedException {
        for(int i=0; i < 10; i++)
            queue.add(new Integer(i));
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
        assertEquals(0, queue.size());
    }


    public void testWaitUntilClosed5() throws QueueClosedException {
        for(int i=0; i < 10; i++)
            queue.add(new Integer(i));
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
        assertTrue(queue.size() > 0);
    }



    public void testRemoveElementNoElement() {
        String s1="Q1";

        try {
            queue.removeElement(s1);
            assertFalse(queue.closed());
            assertEquals(0, queue.size());
        }
        catch(QueueClosedException ex) {
            fail(ex.toString());
        }
    }


    public void testRemoveElementOneElement() {
        String s1="Q1";

        try {
            queue.add(s1);
            queue.removeElement(s1);
            assertEquals(0, queue.size());
            assertTrue(queue.getFirst() == null);
            assertTrue(queue.getLast() == null);
        }
        catch(QueueClosedException ex) {
            fail(ex.toString());
        }
    }

    public void testRemoveElementTwoElementsFirstFound() {
        String s1="Q1", s2="Q2";

        try {
            queue.add(s1);
            queue.add(s2);
            queue.removeElement(s1);
            assertEquals(1, queue.size());
            assertEquals(queue.getFirst(), s2);
            assertEquals(queue.getLast(), s2);
            assertEquals(queue.getFirst(), queue.getLast());
        }
        catch(QueueClosedException ex) {
            fail(ex.toString());
        }
    }

    public void testRemoveElementTwoElementsSecondFound() {
        String s1="Q1", s2="Q2";

        try {
            queue.add(s1);
            queue.add(s2);
            queue.removeElement(s2);
            assertEquals(1, queue.size());
            assertEquals(queue.getFirst(), s1);
            assertEquals(queue.getLast(), s1);
            assertEquals(queue.getFirst(), queue.getLast());
        }
        catch(QueueClosedException ex) {
            fail(ex.toString());
        }
    }

    public void testRemoveElementThreeElementsFirstFound() {
        String s1="Q1", s2="Q2", s3="Q3";

        try {
            queue.add(s1);
            queue.add(s2);
            queue.add(s3);
            queue.removeElement(s1);
            assertEquals(2, queue.size());
            assertEquals(queue.getFirst(), s2);
            assertEquals(queue.getLast(), s3);
        }
        catch(QueueClosedException ex) {
            fail(ex.toString());
        }
    }

    public void testRemoveElementThreeElementsSecondFound() {
        String s1="Q1", s2="Q2", s3="Q3";

        try {
            queue.add(s1);
            queue.add(s2);
            queue.add(s3);
            queue.removeElement(s2);
            assertEquals(2, queue.size());
            assertEquals(queue.getFirst(), s1);
            assertEquals(queue.getLast(), s3);
        }
        catch(QueueClosedException ex) {
            fail(ex.toString());
        }
    }

    public void testRemoveElementThreeElementsThirdFound() {
        String s1="Q1", s2="Q2", s3="Q3";

        try {
            queue.add(s1);
            queue.add(s2);
            queue.add(s3);
            queue.removeElement(s3);
            assertEquals(2, queue.size());
            assertEquals(queue.getFirst(), s1);
            assertEquals(queue.getLast(), s2);
        }
        catch(QueueClosedException ex) {
            fail(ex.toString());
        }
    }


    public void testRemoveAndClose() {
        try {
            new Thread() {
                public void run() {
                    Util.sleep(1000);
                    queue.close(true); // close gracefully
                }
            }.start();

            queue.remove();
            fail("we should not be able to remove an object from a closed queue");
        }
        catch(QueueClosedException ex) {
            assertTrue(ex instanceof QueueClosedException); // of course, stupid comparison...
        }
    }


    public void testRemoveAndCloseWithTimeout() throws TimeoutException {
        try {
            new Thread() {
                public void run() {
                    Util.sleep(1000);
                    queue.close(true); // close gracefully
                }
            }.start();

            queue.remove(5000);
            fail("we should not be able to remove an object from a closed queue");
        }
        catch(QueueClosedException ex) {
            assertTrue(ex instanceof QueueClosedException); // of course, stupid comparison...
        }
        catch(TimeoutException timeout) {
            fail("we should not get a TimeoutException, but a QueueClosedException here");
        }
    }


    public void testInterruptAndRemove() throws QueueClosedException {
        Thread.currentThread().interrupt();
        Object el=null;
        try {
            el=queue.remove(2000);
            fail("we should not get here");
        }
        catch(TimeoutException e) {
            assertNull(el);
        }
    }

    public void testClear() throws QueueClosedException {
        queue.add("one");
        queue.add("two");
        assertEquals(2, queue.size());
        queue.close(true);
        assertEquals(2, queue.size());
        queue.clear();
        assertEquals(0, queue.size());
        queue=new Queue();
        queue.add("one");
        queue.add("two");
        queue.clear();
        assertEquals(0, queue.size());
        queue.add("one");
        queue.add("two");
        assertEquals(2, queue.size());
        queue.clear();
        assertEquals(0, queue.size());
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
    public void testBarrier() {
        RemoveOneItem[] removers=new RemoveOneItem[10];
        int num_dead=0;

        for(int i=0; i < removers.length; i++) {
            removers[i]=new RemoveOneItem(i);
            removers[i].start();
        }

        Util.sleep(1000);

        System.out.println("-- adding element 99");
        try {
            queue.add(new Long(99));
        }
        catch(Exception ex) {
            System.err.println(ex);
        }

        Util.sleep(5000);
        System.out.println("-- adding element 100");
        try {
            queue.add(new Long(100));
        }
        catch(Exception ex) {
            System.err.println(ex);
        }

        Util.sleep(1000);

        for(int i=0; i < removers.length; i++) {
            System.out.println("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
            if(!removers[i].isAlive()) {
                num_dead++;
            }
        }

        assertEquals(2, num_dead);
    }

    /** Multiple threads call remove(), one threads then adds an element. Only 1 thread should actually terminate
     * (the one that has the element) */
    public void testBarrierWithTimeOut()
    {
        RemoveOneItemWithTimeout[] removers = new RemoveOneItemWithTimeout[10];
        int num_dead = 0;

        for (int i = 0; i < removers.length; i++)
        {
            removers[i] = new RemoveOneItemWithTimeout(i, 1000);
            removers[i].start();
        }

        Util.sleep(5000);

        System.out.println("-- adding element 99");
        try
        {
            queue.add(new Long(99));
        }
        catch (Exception ex)
        {
            System.err.println(ex);
        }

        Util.sleep(5000);
        System.out.println("-- adding element 100");
        try
        {
            queue.add(new Long(100));
        }
        catch (Exception ex)
        {
            System.err.println(ex);
        }

        Util.sleep(1000);

        for (int i = 0; i < removers.length; i++)
        {
            System.out.println("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
            if (!removers[i].isAlive())
            {
                num_dead++;
            }
        }

        assertEquals(2, num_dead);

        queue.close(false); // will cause all threads still blocking on peek() to return

        Util.sleep(2000);

        num_dead = 0;
        for (int i = 0; i < removers.length; i++)
        {
            System.out.println("remover #" + i + " is " + (removers[i].isAlive() ? "alive" : "terminated"));
            if (!removers[i].isAlive())
            {
                num_dead++;
            }
        }
        assertEquals(10, num_dead);

    }


    /** Multiple threads add one element, one thread read them all.
     * (the one that has the element) */
    public void testMultipleWriterOneReader()
    {
        AddOneItem[] adders = new AddOneItem[10];
        int num_dead = 0;
        int num_items = 0;
        int items = 1000;

        for (int i = 0; i < adders.length; i++)
        {
            adders[i] = new AddOneItem(i, items);
            adders[i].start();
        }

        while (num_items < (adders.length*items))
        {
            try
            {
                queue.remove();
                num_items++;
            }
            catch (Exception ex)
            {
                System.err.println(ex);
            }
        }

        Util.sleep(1000);

        for (int i = 0; i < adders.length; i++)
        {
            System.out.println("adder #" + i + " is " + (adders[i].isAlive() ? "alive" : "terminated"));
            if (!adders[i].isAlive())
            {
                num_dead++;
            }
        }

        assertEquals(10, num_dead);

        queue.close(false); // will cause all threads still blocking on peek() to return
    }


    /**
     * Times how long it takes to add and remove 1000000 elements concurrently (1 reader, 1 writer)
     */
    public void testConcurrentAddRemove() {
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
            try {
                ret=queue.remove();
                if(ret != null)
                    num_received++;
            }
            catch(QueueClosedException e) {
                e.printStackTrace();
                fail();
            }
        }
        assertEquals(NUM, num_received);
        stop=System.currentTimeMillis();
        System.out.println("time to add/remove " + NUM + " elements: " + (stop-start));
    }



    /** Has multiple threads add(), remove() and peek() elements to/from the queue */
    public void testConcurrentAccess() {
        final int NUM_THREADS=10;
        final int INTERVAL=20000;

        Writer[] writers=new Writer[NUM_THREADS];
        Reader[] readers=new Reader[NUM_THREADS];
        int[] writes=new int[NUM_THREADS];
        int[] reads=new int[NUM_THREADS];
        long total_reads=0, total_writes=0;


        for(int i=0; i < writers.length; i++) {
            readers[i]=new Reader(i, reads);
            readers[i].start();
            writers[i]=new Writer(i, writes);
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

    class AddOneItem extends Thread
    {
        Long retval = null;
        int rank = 0;
        int iteration = 0;

        AddOneItem(int rank, int iteration)
        {
            super("AddOneItem thread #" + rank);
            this.rank = rank;
            this.iteration = iteration;
            setDaemon(true);
        }

        public void run()
        {
            try
            {
                for (int i = 0; i < iteration; i++)
                {
                    queue.add(new Long(rank));
                    // Util.sleepRandom(1);
                    // System.out.println("Thread #" + rank + " added element (" + rank + ")");
                }
            }
            catch (QueueClosedException closed)
            {
                System.err.println("Thread #" + rank + ": queue was closed");
            }
        }

    }

    class RemoveOneItem extends Thread {
        Long retval=null;
        int rank=0;


        RemoveOneItem(int rank) {
            super("RemoveOneItem thread #" + rank);
            this.rank=rank;
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

    class RemoveOneItemWithTimeout extends Thread
    {
        Long retval = null;
        int rank = 0;
        long timeout = 0;

        RemoveOneItemWithTimeout(int rank, long timeout)
        {
            super("RemoveOneItem thread #" + rank);
            this.rank = rank;
            this.timeout=timeout;
            setDaemon(true);
        }

        public void run()
        {
            boolean finished = false;
            while (!finished)
            {
                try
                {
                    retval = (Long) queue.remove(timeout);
                    // System.out.println("Thread #" + rank + " removed element (" + retval + ")");
                    finished = true;
                }
                catch (QueueClosedException closed)
                {
                    System.err.println("Thread #" + rank + ": queue was closed");
                    finished = true;
                }
                catch (TimeoutException e)
                {
                }
            }
        }

        Long getRetval()
        {
            return retval;
        }
    }




    class Writer extends Thread {
        int rank=0;
        int num_writes=0;
        boolean running=true;
        int[] writes=null;

        Writer(int i, int[] writes) {
            super("WriterThread");
            rank=i;
            this.writes=writes;
            setDaemon(true);
        }


        public void run() {
            while(running) {
                try {
                    queue.add(new Long(System.currentTimeMillis()));
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


    class Reader extends Thread {
        int rank;
        int num_reads=0;
        int[] reads=null;
        boolean running=true;


        Reader(int i, int[] reads) {
            super("ReaderThread");
            rank=i;
            this.reads=reads;
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
                    assertNotNull(el);
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


    public static void main(String[] args) {
        String[] testCaseName={QueueTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}
