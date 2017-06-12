package org.jgroups.tests;

import org.jgroups.util.RingBuffer;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Bela Ban
 * @since  4.0
 */
@Test
public class RingBufferTest {


    public void testEmpty() {
        RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        System.out.println("rb = " + rb);
        //noinspection SizeReplaceableByIsEmpty
        assert rb.size() == 0;
        assert rb.isEmpty();
        assert rb.readIndex() == rb.writeIndex();
    }

    public void testPutAndTake() throws Exception {
        RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        rb.put(1).put(2);
        System.out.println("rb = " + rb);
        assert rb.size() == 2;
        assert !rb.isEmpty();
        rb.put(3).put(4);
        for(int i=1; i <= 4; i++) {
            int num=rb.take();
            assert num == i;
        }
        for(int num : Arrays.asList(5, 6, 7, 8, 9, 10))
            rb.put(num);
        System.out.println("rb = " + rb);
        assert rb.size() == 6;

        for(int num : Arrays.asList(5, 6, 7, 8, 9, 10)) {
            int n=rb.take();
            assert num == n;
        }
        System.out.println("rb = " + rb);
    }


    public void testTakeBlocking() throws InterruptedException {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        new Thread(() -> {
            Util.sleep(1000);
            try {
                rb.put(50);
            }
            catch(InterruptedException e) {
            }
        }).start();
        int num=rb.take();
        System.out.println("num = " + num);
        assert num == 50;
    }

    public void testPutBlocking() throws InterruptedException {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        for(int i=1; i <= 8; i++)
            rb.put(i);

        new Thread(() -> {
            Util.sleep(1000);
            try {
                rb.take();
            }
            catch(InterruptedException e) {
            }
        }).start();
        rb.put(9); // this blocks first until the read() above has completed
        System.out.println("rb = " + rb);
        assert rb.size() == 8;
        int num=rb.take();
        assert num == 2;
    }

    public void testWaitForMessages() throws Exception {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);

        new Thread(() -> {
            Util.sleep(2000);
            try {
                rb.put(99);
            }
            catch(InterruptedException e) {
            }
        }).start();

        int num=rb.waitForMessages();
        assert num == 1;
        num=rb.take();
        assert num == 99;
    }

    public void testWaitForMessagesAndInterrupt() throws Exception {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);

        final Thread current=Thread.currentThread();

        new Thread(() -> {
            Util.sleep(2000);
            current.interrupt();
        }).start();

        try {
            rb.waitForMessages();
            assert false : "should have thrown InterruptedException";
        }
        catch(InterruptedException ex) {
            System.out.printf("got %s as expected\n", ex);
        }
    }

    public void testWaitForMessagesAndInterruptBefore() throws Exception {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        Thread.currentThread().interrupt();
        try {
            rb.waitForMessages();
            assert false : "should have thrown InterruptedException";
        }
        catch(InterruptedException ex) {
            System.out.printf("got %s as expected\n", ex);
        }
    }

    public void testDrainToCollection() throws Exception {
        RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        for(int i=1; i <= 6; i++)
            rb.put(i);
        List<Integer> list=new ArrayList<>(2);
        int drained=rb.drainTo(list, 2);
        System.out.println("list = " + list);
        assert drained == 2;
        assert rb.size() == 4;
        assert list.get(0) == 1 && list.get(1) == 2;
        list.clear();
        drained=rb.drainTo(list);
        System.out.println("list = " + list);
        assert rb.isEmpty();
        assert list.size() == 4;
        for(int i : Arrays.asList(3, 4, 5, 6))
            assert list.remove(0) == i;
    }

    public void testDrainToOnEmptyRingBuffer() {
        RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        List<Integer> list=new ArrayList<>(2);
        int num=rb.drainTo(list);
        assert num == 0;
    }

    public void testDrainToCollectionBlocking() throws Exception {
        RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        for(int i=1; i <= 6; i++)
            rb.put(i);
        List<Integer> list=new ArrayList<>(2);
        int drained=rb.drainToBlocking(list, 2);
        System.out.println("list = " + list);
        assert drained == 2;
        assert rb.size() == 4;
        assert list.get(0) == 1 && list.get(1) == 2;
        list.clear();
        drained=rb.drainToBlocking(list);
        System.out.println("list = " + list);
        assert rb.isEmpty();
        assert list.size() == 4;
        for(int i : Arrays.asList(3, 4, 5, 6))
            assert list.remove(0) == i;
    }


    public void testDrainToCollectionBlocking2() throws Exception {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        List<Integer> list=new ArrayList<>();

        new Thread(() -> {
            Util.sleep(2000);
            try {
                rb.put(99);
            }
            catch(InterruptedException e) {
            }
        }).start();

        int num=rb.drainToBlocking(list);
        assert num == 1;
        assert list.get(0) == 99;
    }

    public void testDrainToCollectionBlockingAndInterrupt() throws Exception {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        List<Integer> list=new ArrayList<>();
        final Thread current=Thread.currentThread();

        new Thread(() -> {
            Util.sleep(2000);
            current.interrupt();
        }).start();

        try {
            rb.drainToBlocking(list);
            assert false : "should have thrown InterruptedException";
        }
        catch(InterruptedException ex) {
            System.out.printf("got %s as expected\n", ex);
        }
    }


    public void testDrainToCollectionBlockingAndInterrupt2() {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        List<Integer> list=new ArrayList<>();
        Thread.currentThread().interrupt();
        try {
            rb.drainToBlocking(list);
            assert false : "should have thrown InterruptedException";
        }
        catch(InterruptedException ex) {
            System.out.printf("got %s as expected\n", ex);
        }
    }

    public void testDrainToArray() throws Exception {
        RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        for(int i=1; i <= 6; i++)
            rb.put(i);
        Integer[] array=new Integer[2];
        int num=rb.drainTo(array);
        System.out.printf("array: %s\n", Util.printListWithDelimiter(array, ", ", 10));
        assert num == 2;
        assert array[0] == 1;
        assert array[1] == 2;

        array=new Integer[10];
        num=rb.drainTo(array);
        System.out.printf("array: %s\n", Util.printListWithDelimiter(array, ", ", 4));
        assert num == 4;
        for(int i=0; i < 4; i++)
            assert array[i] == i + 3;
    }


    public void testDrainToArrayBlocking() throws Exception {
        RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        for(int i=1; i <= 6; i++)
            rb.put(i);
        Integer[] array=new Integer[2];
        int drained=rb.drainToBlocking(array);
        System.out.println("array = " + Util.printListWithDelimiter(array, ", ", 2));
        assert drained == 2;
        assert rb.size() == 4;
        assert array[0] == 1 && array[1] == 2;
        array=new Integer[4];
        drained=rb.drainToBlocking(array);
        System.out.println("array = " + Util.printListWithDelimiter(array, ", ", 4));
        assert rb.isEmpty();
        for(int i=0; i < 4; i++)
            assert array[i] == i +3;
    }


    public void testDrainToArrayBlocking2() throws Exception {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        Integer[] array={0};

        new Thread(() -> {
            Util.sleep(2000);
            try {
                rb.put(99);
            }
            catch(InterruptedException e) {
            }
        }).start();

        int num=rb.drainToBlocking(array);
        assert num == 1;
        assert array[0] == 99;
    }

    public void testDrainToArrayBlockingAndInterrupt() throws Exception {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        Integer[] array={0};
        final Thread current=Thread.currentThread();

        new Thread(() -> {
            Util.sleep(2000);
            current.interrupt();
        }).start();

        try {
            rb.drainToBlocking(array);
            assert false : "should have thrown InterruptedException";
        }
        catch(InterruptedException ex) {
            System.out.printf("got %s as expected\n", ex);
        }
    }


    public void testDrainToCollectionArrayAndInterrupt2() {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        Integer[] array={0};
        Thread.currentThread().interrupt();
        try {
            rb.drainToBlocking(array);
            assert false : "should have thrown InterruptedException";
        }
        catch(InterruptedException ex) {
            System.out.printf("got %s as expected\n", ex);
        }
    }


}

