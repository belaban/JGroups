package org.jgroups.tests;

import org.jgroups.util.SizeBoundedQueue;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Tests {@link org.jgroups.util.SizeBoundedQueue}
 * @author Bela Ban
 * @since  4.0.4
 */
@Test
public class SizeBoundedQueueTest {

    public void testAdd() throws InterruptedException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(7000);
        queue.add("one", 1000);
        System.out.println("queue = " + queue);
        assert queue.size() == 1000;
        assert queue.getElements() == 1;

        queue.add("two", 3000);
        queue.add("three", 3000);
        System.out.println("queue = " + queue);
        assert queue.size() == 7000;
        assert queue.getElements() == 3;
        queue.clear(false);
        queue.add("four", 0); // empty element
        assert queue.getElements() == 1 && queue.isEmpty();
    }

    public void testBlockingAdd() throws InterruptedException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(7000);
        queue.add("one", 1000);
        queue.add("two", 3000);
        queue.add("three", 3000);
        System.out.println("queue = " + queue);

        new Thread(() -> {
            Util.sleep(2000);
            queue.remove();
        }).start();

        queue.add("four", 500);
        assert queue.size() == 6500;
        assert queue.getElements() == 3;
        assert !queue.hasWaiters();
    }

    public void testBlockingAddAndInterruption() throws InterruptedException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(7000);
        final Thread main_thread=Thread.currentThread();
        new Thread(() -> {
            Util.sleep(2000);
            main_thread.interrupt();

        }).start();
        try {
            queue.add("big", 7500);
            assert false: "interrupted add() should have throw exception";
        }
        catch(InterruptedException iex) {
            System.out.printf("received %s as expected\n", iex.getClass().getSimpleName());
            assert queue.isEmpty() && queue.getElements() == 0;
        }
    }

    public void testMultipleAdders() {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(1000);
        Thread[] adders=new Thread[4];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Thread(() -> {
                try {
                    queue.add(String.valueOf(Thread.currentThread().getId()), 1024);
                }
                catch(InterruptedException e) {
                    System.out.printf("Thread %d was interrupted\n", Thread.currentThread().getId());
                }
            });
        }

        for(Thread adder: adders)
            adder.start();

        Util.sleep(2000);
        System.out.println("Interrupting adders");
        for(Thread adder: adders)
            adder.interrupt();
        assert queue.isEmpty();
    }

    public void testMultipleAddersAndClear() throws TimeoutException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(1000);
        Thread[] adders=new Thread[4];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Thread(() -> {
                try {
                    queue.add(String.valueOf(Thread.currentThread().getId()), 1024);
                }
                catch(InterruptedException e) {
                    System.out.printf("Thread %d was interrupted\n", Thread.currentThread().getId());
                }
            });
        }

        for(Thread adder: adders)
            adder.start();
        Util.waitUntil(10000, 500, () -> Arrays.stream(adders).allMatch(t -> t.getState() == Thread.State.WAITING));

        queue.clear(true);
        Util.waitUntil(10000, 500, () -> Arrays.stream(adders).allMatch(t -> t.getState() == Thread.State.TERMINATED));
        assert queue.isEmpty() && queue.isDone();
    }


    public void testClear() throws InterruptedException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(7000);
        queue.add("one", 1000);
        queue.add("two", 3000);
        queue.add("three", 3000);
        queue.clear(false);
        assert queue.isEmpty() && queue.getElements() == 0;
    }

    public void testClear2() throws InterruptedException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(7000);
        queue.add("one", 1000);
        queue.add("two", 3000);
        queue.add("three", 3000);
        queue.clear(true);
        assert queue.isEmpty() && queue.getElements() == 0 && queue.isDone();
    }


    public void testDrainAll() throws InterruptedException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(7000);
        queue.add("one", 1000);
        queue.add("two", 3000);
        queue.add("three", 3000);
        List<String> list=new ArrayList<>();
        int drained=queue.drainTo(list, 20000);
        assert drained == 7000;
        assert queue.isEmpty() && queue.getElements() == 0;
        assert list.size() == 3;
    }

    public void testPartialDrain() throws InterruptedException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(7000);
        queue.add("one", 1000);
        queue.add("two", 3000);
        queue.add("three", 3000);
        List<String> list=new ArrayList<>();
        int drained=queue.drainTo(list, 5000);
        assert drained == 4000;
        assert queue.size() == 3000 && queue.getElements() == 1;
        assert list.size() == 2;
    }

    public void testDrainToSeparateThread() throws InterruptedException {
        SizeBoundedQueue<String> queue=new SizeBoundedQueue<>(7000);
        queue.add("one", 1000);
        queue.add("two", 3000);
        queue.add("three", 3000);
        new Thread(() -> {
            Util.sleep(2000);
            queue.drainTo(new ArrayList<>(), 20000);

        }).start();
        queue.add("four", 1000);
        queue.add("five", 1000);
        System.out.println("queue = " + queue);
        assert queue.size() == 2000;
        assert queue.getElements() == 2;
    }

    /**
     * The queue has elements 1 and 2, then we drain a max of 1000 bytes, so we remove 1 element. 1 element is left in
     * the queue
     */
    public void testWaitersOnQueue() throws InterruptedException {
        SizeBoundedQueue<Integer> queue=new SizeBoundedQueue<>(2000);
        waitersOnQueue(queue, 2, 0, 1000, 1000, 1);
        assert queue.getElements() == 1;
    }

    /**
     * The queue has elements 1, 2 and 3 and is then full. 2 subsequent threads are blocked adding messages 4 and 5.
     * When draining the queue, we need to get messages 1-5
     */
    public void testWaitersOnQueue2() throws InterruptedException {
        SizeBoundedQueue<Integer> queue=new SizeBoundedQueue<>(3000);
        waitersOnQueue(queue, 3, 2, 1000, 10000, 5);
        assert queue.isEmpty();
    }

    /**
     * 2 elements on queue, 3 adders waiting. Drain with max_drain_size of 4: result should be 4 elements in the
     * list and a queue size of 1
     */
    public void testWaitersOnQueue3() throws InterruptedException {
        SizeBoundedQueue<Integer> queue=new SizeBoundedQueue<>(2000);
        waitersOnQueue(queue, 2, 3, 1000, 4000, 4);
        assert queue.getElements() == 1;
        assert queue.remove() == 5;
    }

    /** Queue of 1 and 4 adders. Remove size is 4 */
    public void testWaitersOnQueue4() throws InterruptedException {
        SizeBoundedQueue<Integer> queue=new SizeBoundedQueue<>(1000);
        waitersOnQueue(queue, 1, 4, 1000, 4000, 4);
        assert queue.getElements() == 1;
        assert queue.remove() == 5;
    }

    /**
     * 2 elements in queue, 1 waiter. Drain 2 elements. Queue should have size of 1
     */
    public void testWaitersOnQueue5() throws InterruptedException {
        SizeBoundedQueue<Integer> queue=new SizeBoundedQueue<>(2000);
        waitersOnQueue(queue, 2, 1, 1000, 2000, 2);
        assert queue.getElements() == 1;
        assert queue.remove() == 3;
    }


    protected static void waitersOnQueue(SizeBoundedQueue<Integer> queue, int initial_adds, int num_adders, int element_size,
                                         int max_drain_size, int expected_drained) throws InterruptedException {
        int total_adds=initial_adds + num_adders;
        for(int i=1; i <= initial_adds; i++)
            queue.add(i, element_size);
        assert queue.getElements() == initial_adds && queue.size() == initial_adds * element_size;

        if(num_adders > 0 ) {
            Thread[] adders=new Thread[num_adders];
            for(int i=0; i < adders.length; i++) {
                final int j=i;
                adders[i]=new Thread(() -> {
                    try {
                        queue.add(j + initial_adds + 1, element_size);
                    }
                    catch(InterruptedException e) {
                    }
                });
                adders[i].start();
            }
            for(int i=0; i < 10; i++) {
                boolean ok=true;
                for(Thread t : adders) {
                    Thread.State state=t.getState();
                    switch(state) {
                        case NEW:
                        case RUNNABLE:
                        case TERMINATED:
                            ok=false;
                            break;
                    }
                }
                if(ok)
                    break;
            }
        }

        assert queue.getWaiters() == num_adders;
        List<Integer> list=new ArrayList<>(total_adds);
        int drained=queue.drainTo(list, max_drain_size);
        assert drained == expected_drained * element_size && list.size() == expected_drained;
        for(int i=1; i <= expected_drained; i++)
            assert list.contains(i);
    }
}
