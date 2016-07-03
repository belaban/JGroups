package org.jgroups.tests;

import org.jgroups.util.RingBuffer;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;

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

    public void testWriteAndRead() throws Exception {
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
        for(int num: Arrays.asList(5,6,7,8,9,10))
            rb.put(num);
        System.out.println("rb = " + rb);
        assert rb.size() == 6;

        for(int num: Arrays.asList(5,6,7,8,9,10)) {
            int n=rb.take();
            assert num == n;
        }
        System.out.println("rb = " + rb);
    }


    public void testReadBlocking() throws InterruptedException {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        new Thread(()-> {Util.sleep(1000);
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

    public void testWriteBlocking() throws InterruptedException {
        final RingBuffer<Integer> rb=new RingBuffer<>(Integer.class, 8);
        for(int i=1; i <= 8; i++)
            rb.put(i);

        new Thread(()-> {Util.sleep(1000);
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
}
