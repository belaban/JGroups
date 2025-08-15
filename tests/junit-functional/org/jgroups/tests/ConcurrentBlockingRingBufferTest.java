package org.jgroups.tests;

import org.jgroups.util.ConcurrentBlockingRingBuffer;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This used to test the old RingBuffer (removed in 5.5.0); retrofitted to test
 * {@link org.jgroups.util.ConcurrentBlockingRingBuffer}
 * @author Bela Ban
 */
@Test(singleThreaded=true)
public class ConcurrentBlockingRingBufferTest {
    protected ConcurrentBlockingRingBuffer<Integer> rb;

    @BeforeMethod
    protected void setup() {
        rb=ConcurrentBlockingRingBuffer.createBlocking(8);
    }

    @AfterMethod
    protected void destoy() {
        rb.clear();
        rb=null;
    }

    public void testEmpty() {
        System.out.println("rb = " + rb);
        //noinspection SizeReplaceableByIsEmpty
        assert rb.size() == 0;
        assert rb.isEmpty();
        assert rb.readIndex() == rb.writeIndex();
    }

    public void testPutAndTake() throws Exception {
        rb.put(1);
        rb.put(2);
        System.out.println("rb = " + rb);
        assert rb.size() == 2;
        assert !rb.isEmpty();
        rb.put(3);
        rb.put(4);
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
        new Thread(() -> {
            Util.sleep(2000);
            try {
                rb.put(99);
            }
            catch(InterruptedException e) {
            }
        }).start();

        int num=rb.take(); // blocks until a message is available
        assert num == 99;
    }

    public void testWaitForMessagesAndInterrupt() throws Exception {
        final Thread current=Thread.currentThread();
        new Thread(() -> {
            Util.sleep(2000);
            current.interrupt();
        }).start();

        try {
            int ignored=rb.take();
            assert false : "should have thrown InterruptedException";
        }
        catch(InterruptedException ex) {
            System.out.printf("got %s as expected\n", ex);
        }
    }

    public void testWaitForMessagesAndInterruptBefore() throws Exception {
        Thread.currentThread().interrupt();
        try {
            int ignored=rb.take();
            assert false : "should have thrown InterruptedException";
        }
        catch(InterruptedException ex) {
            System.out.printf("got %s as expected\n", ex);
        }
    }

    public void testDrainToCollection() throws Exception {
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
        List<Integer> list=new ArrayList<>(2);
        int num=rb.drainTo(list);
        assert num == 0;
    }

    public void testDrainToCollection2() throws Exception {
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

}

