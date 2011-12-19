package org.jgroups.tests;

import org.jgroups.util.RingBuffer;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * @author Bela Ban
 * @since 3.1
 */
// todo: add tests for long overflow (can become negative)
@Test(description="Functional tests for the RingBuffer class")
public class RingBufferTest {

    public void testConstructor() {
        RingBuffer buf=new RingBuffer(100, 1);
        System.out.println("buf = " + buf);
        assert buf.capacity() == 100;
    }

    public void testAddWithInvalidSeqno() {
        RingBuffer<Integer> buf=new RingBuffer<Integer>(100, 20);
        assert buf.add(10, 0) == false;
        assert buf.add(20, 0) == false;
    }

    public void testAdd() {
        RingBuffer<Integer> buf=new RingBuffer<Integer>(100, 0);
        buf.add(1, 322649);
        buf.add(2, 100000);
        System.out.println("buf = " + buf);
        assert buf.size() == 2;
    }

    public void testAddWithWrapAround() {
        RingBuffer<Integer> buf=new RingBuffer<Integer>(10, 5);
        for(int i=6; i <=15; i++)
            assert buf.add(i, i) : "addition of seqno " + i + " failed";
        System.out.println("buf = " + buf);
        for(int i=0; i < 3; i++) {
            Integer val=buf.remove();
            System.out.println("removed " + val);
            assert val != null;
        }
        System.out.println("buf = " + buf);

        buf.stable(8);
        System.out.println("buf = " + buf);
        for(int i=16; i <= 18; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);
    }

    public void testAddBeyondCapacity() {
        RingBuffer<Integer> buf=new RingBuffer<Integer>(10, 0);
        for(int i=1; i <=10; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);
    }

    public void testAddMissing() {
        RingBuffer<Integer> buf=new RingBuffer<Integer>(10, 0);
        for(int i: Arrays.asList(1,2,4,5,6))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        assert buf.size() == 5 && buf.missing() == 1;

        Integer num=buf.remove();
        assert num == 1;
        num=buf.remove();
        assert num == 2;
        num=buf.remove();
        assert num == null;

        buf.add(3, 3);
        System.out.println("buf = " + buf);
        assert buf.size() == 4 && buf.missing() == 0;

        for(int i=3; i <= 6; i++) {
            num=buf.remove();
            System.out.println("buf = " + buf);
            assert num == i;
        }

        num=buf.remove();
        assert num == null;
    }



}
