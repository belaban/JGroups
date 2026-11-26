package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.RequestTable;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.LongStream;

/**
 * Tests {@link RequestTable}
 * @author Bela Ban
 * @since  3.6.7
 */
@Test(groups=Global.FUNCTIONAL)
public class RequestTableTest {

    public void testSimpleCreation() {
        RequestTable<Integer> table=new RequestTable<>(100);
        assert table.size() == 0 && table.high() == 0 && table.low() == 0;
        assert table.capacity() == Util.getNextHigherPowerOfTwo(100);
    }

    public void testIndex() {
        RequestTable<Integer> table=new RequestTable<>(8);
        assert table.capacity() == 8;
        for(int i=0; i < 20; i++) {
            int index=table.index(i);
            int wrap_around=i % table.capacity();
            String format=String.format("seqno=%d, index=%d, wrap_around=%d", i, index, wrap_around);
            System.out.println(format);
            assert index == wrap_around : format;
        }
    }

    public void testAdd() {
        RequestTable<Integer> table=create(4, 0, 4);
        System.out.println("table: " + table);
        assertBounds(table, 4, 4);
    }

    public void testAdd2() {
        RequestTable<Integer> table=create(4, 0, 4);
        remove(table, 0, 2); // removes 0, 1
        add(table, 4,6); // adds 4, 5, wraps around
        assertBounds(table, 4, 4);
    }

    public void testAddWithOffset() {
        RequestTable<Integer> table=new RequestTable<>(8, 1, 1);
        add(table, 1, 8);
        System.out.println("table = " + table);
        assertBounds(table, 8, 7);
        add(table, 8, 12);
        System.out.println("table = " + table);
        assertBounds(table, 16, 11);
        remove(table, 0, 20);
        add(table, 12, 15);
        boolean rc=table.compact();
        assert rc;
        assertBounds(table, 8, 3);
    }

    public void testAddAndGrow() {
        RequestTable<Integer> table=create(4, 0, 4);
        for(int i=4; i <= 10; i++) {
            long seqno=table.add(i);
            assert seqno == i;
        }
        System.out.println("table = " + table);
        assertBounds(table, Util.getNextHigherPowerOfTwo(11), 11);
        assert table.low() == 0 && table.high() == 11;
    }

    public void testAddAndGrow2() {
        RequestTable<Integer> table=create(4, 0, 4);
        remove(table, 0, 10);
        add(table, 4, 12);
        checkIndices(table, 4, 12);
        add(table, 12, 20); // grows array again
        checkIndices(table, 4, 20);
        table.add(20); // grows array
        checkIndices(table, 4, 21);
    }


    public void testAddRemove() {
        RequestTable<Integer> table=create(8, 0, 8);
        System.out.println("table = " + table);
        remove(table, 1, 8);
        System.out.println("table = " + table);
        table.add(8); // will trigger a growth
        System.out.println("table = " + table);
        assert table.size() == 2; // 0 and 8
        assert table.capacity() == 16;

        add(table, 9, 100);
        System.out.println("table = " + table);
        assert table.size() == 93;
        assert table.capacity() == Util.getNextHigherPowerOfTwo(100);

        table.remove(0);
        boolean rc=table.compact();
        assert !rc;
        remove(table, 8, 50);
        System.out.println("rc = " + rc);
        assert table.size() == 50;

        rc=table.compact();
        System.out.println("table = " + table);
        assert rc;
        assert table.size() == 50;
        assert table.capacity() == 64;
    }


    public void testAddMany() {
        RequestTable<Integer> table=create(4, 0, 0);
        add(table, 0, 1000); // adds [0 .. 999]
        System.out.println("table = " + table);
        assertBounds(table, 1024, 1000);
        assert table.low() == 0 && table.high() == 1000;

        remove(table, 1, 1000);
        System.out.println("table = " + table);
        assertBounds(table, 1024, 1);
        assert table.low() == 0 && table.high() == 1000;

        table.remove(0);
        System.out.println("table = " + table);
        assertBounds(table, 1024, 0);
        assert table.low() == 1000 && table.high() == 1000;

        add(table, 1000, 2000);
        System.out.println("table = " + table);
        assertBounds(table, 1024, 1000);
        assert table.low() == 1000 && table.high() == 2000;

        remove(table, 800, 2200);
        System.out.println("table = " + table);
        assertBounds(table, 1024, 0);
        assert table.low() == 2000 && table.high() == 2000;
    }

    public void testAddAndRemove() {
        RequestTable<Integer> table=create(4, 0, 4);
        remove(table, 0, 3);
        assert table.low() == 3 && table.high() == 4;
        add(table, 4, 7); // adds 4,5,6
        assertBounds(table, 4, 4);
        assert table.low() == 3 && table.high() == 7;

        table.add(7); // this grows the array to 8
        assertBounds(table, 8, 5);
        assert table.low() == 3 && table.high() == 8;
        checkIndices(table, table.low(), table.high());
    }

    public void testRemove() {
        RequestTable<Integer> table=new RequestTable<>(1);
        Integer el=table.remove(0);
        assert el == null;
    }

    public void testRemoveOne() {
        RequestTable<Integer> table=create(1, 0, 1);
        assertBounds(table, 1, 1);
        Integer el=table.remove(0);
        assert el != null && el == 0;
        assert table.low() == 1 && table.high() == 1;
    }

    public void testRemoveAll() {
        RequestTable<Integer> table=create(4, 0, 4);
        remove(table, 1, 3);
        assertBounds(table, 4, 2);
        assert table.low() == 0 && table.high() == 4;
        table.remove(0);
        assertBounds(table, 4, 1);
        assert table.low() == 3 && table.high() == 4;
        table.remove(3);
        assertBounds(table, 4, 0);
        assert table.low() == 4 && table.high() == 4;
    }

    public void testRemoveMany() {
        RequestTable<Integer> table=create(40, 0, 41); // exclusive 41
        remove(table, 0, 4);
        table.removeMany(LongStream.rangeClosed(4, 40), seqno -> System.out.printf("seqno=%d\n", seqno));
        assert table.size() == 0;
        assert table.low() == 41 && table.high() == 41;
    }

    public void testClear() {
        RequestTable<Integer> table=create(4, 0, 0);
        add(table, 0, 100);
        assertBounds(table, 128, 100);
        remove(table, 0, 50);
        assert table.low() == 50 && table.high() == 100;
        table.clear();
        assert table.low() == 0 && table.high() == 0;
        assert table.size() == 0;
    }

    public void testConcurrentRemoval() throws Exception {
        RequestTable<Integer> table=create(4, 0, 1000);
        List<Integer> tmp=new ArrayList<>(1000);
        for(int i=0; i < 1000; i++)
            tmp.add(i);
        Collections.shuffle(tmp);
        ConcurrentLinkedQueue<Integer> list=new ConcurrentLinkedQueue<>(tmp);
        Remover[] removers=new Remover[10];
        final CountDownLatch latch=new CountDownLatch(1);
        for(int i=0; i < removers.length; i++) {
            removers[i]=new Remover(table, latch, list);
            removers[i].start();
        }

        int total_removed=0;
        latch.countDown();
        for(Remover remover: removers) {
            remover.join();
            total_removed+=remover.num_removed;
        }
        System.out.printf("table: %s, total removed=%d\n", table, total_removed);
        assertBounds(table, 1024, 0);
        assert table.low() == 1000 && table.high() == 1000;
        assert total_removed == 1000;
    }

    public void testGrow() {
        RequestTable<Integer> table=create(4, 0, 4);
        assert table.capacity() == 4;
        table.grow(5);
        assert table.capacity() == 8;
        checkIndices(table, 0, 4);
    }

    public void testCompact() {
        RequestTable<Integer> table=create(8, 0, 4);
        boolean rc=table.compact();
        assert rc;
        assertBounds(table, 4, 4);
        rc=table.compact();
        assert !rc;
        remove(table, 0, 3);
        rc=table.compact();
        assert rc;
        assertBounds(table, 2, 1);
    }

    public void testCompact2() {
        RequestTable<Integer> table=create(8, 0, 8);
        remove(table, 0, 6); // remove [0 .. 5], 6 and 7 are still present
        add(table, 8, 10); // wraps around for 8, 9
        boolean rc=table.compact();
        assert rc;
        assertBounds(table, 4, 4);
        checkIndices(table, table.low(), table.high());
    }

    public void testCompact3() {
        RequestTable<Integer> table=create(8, 0, 8);
        remove(table, 1,5);
        boolean rc=table.compact();
        assert !rc;
        checkIndices(table, 8,4);
    }

    public void testCompact4() {
        RequestTable<Integer> table=new RequestTable<>(1024, 1, 1);
        add(table, 1, 1023);
        table.remove(1); // low=2
        remove(table, 3, 1002);
        System.out.println("table = " + table);
        assert table.get(2) == 2;
        for(int i=1002; i < 1023; i++)
            assert table.get(i) == i : String.format("expected %d but got %d", i, table.get(i));

        boolean rc=table.compact();
        assert !rc;
        assertBounds(table, 1024, 22);
        assert table.get(2) == 2;
        for(int i=1002; i < 1023; i++)
            assert table.get(i) == i : String.format("expected %d but got %d", i, table.get(i));

        table.add(1023);
        assertBounds(table, 1024, 23);
        assert table.get(2) == 2;
        for(int i=1002; i < 1024; i++)
            assert table.get(i) == i : String.format("expected %d but got %d", i, table.get(i));

    }

    public void testContiguousSpaceAvailable() {
        RequestTable<Integer> table=create(8, 0, 8);
        boolean rc=table.contiguousSpaceAvailable();
        assert !rc;
        remove(table, 5,8);
        rc=table.contiguousSpaceAvailable();
        assert !rc;
        table.remove(1); table.remove(4);
        rc=table.contiguousSpaceAvailable();
        assert rc;
        rc=table.compact();
        assert !rc;
    }

    public void testContiguousSpaceAvailable2() {
        RequestTable<Integer> table=create(8, 0, 8);
        boolean rc=table.contiguousSpaceAvailable();
        assert !rc;
        remove(table, 6,8);
        rc=table.contiguousSpaceAvailable();
        assert !rc;
        remove(table, 0, 3);
        rc=table.contiguousSpaceAvailable();
        assert !rc;
    }

    public void testCompactToZeroAndSubsequentAddition() {
        RequestTable<Integer> table=create(8, 0, 8);
        remove(table, 0, 10);
        assert table.size() == 0;
        while(table.compact()) ;
        add(table, 8, 20);
        assertBounds(table, 16, 12);
        for(int i=8; i < 20; i++) {
            Integer num=table.remove(i);
            assert num != null && num == i;
        }
        assertBounds(table, 16, 0);
    }

    public void testCompactTriggeredByRemoves() {
        RequestTable<Integer> table=create(4, 0, 0);
        add(table, 0, 10000);
        remove(table, 0, 9000);
        System.out.println("table = " + table);
        table.removesTillCompaction(10);
        int cap=table.capacity();
        assert cap == Util.getNextHigherPowerOfTwo(10000);

        remove(table, 9000, 9012);
        cap/=2;
        assert table.capacity() == Util.getNextHigherPowerOfTwo(cap);

        remove(table, 9000, 10000);
        assert table.size() == 0;

        while(table.compact()) ;
        assertBounds(table, 0, 0);
        System.out.println("table = " + table);

        add(table, 10000, 10010);
        System.out.println("table = " + table);
        assertBounds(table, 16, 10);
    }

    public void testMultipleGrowAndCompact() {
        RequestTable<Integer> table=create(100, 0, 100);
        remove(table, 0, 90);
        add(table, 100, 110);
        for(int i=1; i <= 4; i++) {
            System.out.printf("#%d: current capacity: %d", i, table.capacity());
            table.grow(table.capacity() + 1);
            System.out.printf(", new capacity: %d\n", table.capacity());
        }

        System.out.println("Compacting table:");
        while(true) {
            int old_cap=table.capacity();
            boolean rc=table.compact();
            if(!rc)
                break;
            System.out.printf("Compacted table from %d -> %d\n", old_cap, table.capacity());
            System.out.print("Checking contents: ");
            checkIndices(table, table.low(), table.high());
            System.out.println("OK");
        }
    }

    public void testMultipleGrowAndCompact2() {
        RequestTable<Integer> table=create(100, 0, 100);
        remove(table, 10, 90);
        assertBounds(table, 128, 20);
        checkIndices(table, 0, 10);
        checkIndices(table, 90, 99);

        boolean rc=table.compact();
        assert !rc;
        assertBounds(table, 128, 20);
        checkIndices(table, 0, 10);
        checkIndices(table, 90, 99);
    }

    public void testForEach() {
        RequestTable<Integer> table=create(100, 0, 100);
        for(int i=0; i < 100; i++)
            if(i % 2 == 0) // removes odd elements
                table.remove(i);
        CountNonNullElements counter=new CountNonNullElements();
        table.forEach(counter);
        int num=counter.num;
        assert num == 50 : String.format("expected 50 but got %d non-null elements", num);
    }

    public void testForEach2() {
        RequestTable<Integer> table=create(100, 0, 100);
        remove(table, 10, 90);
        boolean rc=table.compact();
        assert !rc;
        CountNonNullElements counter=new CountNonNullElements();
        table.forEach(counter);
        int num=counter.num;
        assert num == 20 : String.format("expected 20 but got %d non-null elements", num);
    }

    public void testForEach3() {
        RequestTable<Integer> table=create(60, 0, 60);
        remove(table, 0, 50);
        add(table, 60, 70);
        boolean rc=table.compact();
        assert rc;
        CountNonNullElements counter=new CountNonNullElements();
        table.forEach(counter);
        int num=counter.num;
        assert num == 20 : String.format("expected 20 but got %d non-null elements", num);
        checkIndices(table, table.low(), table.high());
    }

    public void testForEach4() {
        RequestTable<Integer> table=create(4, 0, 4);
        CountNonNullElements counter=new CountNonNullElements();
        table.forEach(counter);
        int num=counter.num;
        assert num == 4 : String.format("expected 4 but got %d non-null elements", num);
    }



    protected static RequestTable<Integer> create(int capacity, int from, int to) {
        RequestTable<Integer> table=new RequestTable<>(capacity);
        add(table, from, to);
        return table;
    }

    protected static void add(RequestTable<Integer> table, int from, int to) {
        for(int i=from; i < to; i++)
            table.add(i);
    }

    protected static void remove(RequestTable<Integer> table, int from, int to) {
        for(int i=from; i < to; i++)
            table.remove(i);
    }

    protected static <T> void assertBounds(RequestTable<T> table, int capacity, int size) {
        assert capacity < 0 || table.capacity() == capacity : String.format("table.cap=%d, expected cap=%d", table.capacity(), capacity);
        assert size < 0 || table.size() == size : String.format("table.size=%d, expected size=%d", table.size(), size);
    }

    protected static void checkIndices(RequestTable<Integer> table, long low, long high) {
        for(long i=low; i < high; i++) {
            Integer num=table.get(i);
            System.out.printf("%d: %d\n", i, num);
            assert num != null && num == i : String.format("expected %d but got %d", i, num);
        }
    }

    protected static class Remover extends Thread {
        protected final RequestTable<Integer> table;
        protected final CountDownLatch        latch;
        protected final Queue<Integer>        list;
        protected int                         num_removed;

        public Remover(RequestTable<Integer> table, CountDownLatch latch, Queue<Integer> list) {
            this.table=table;
            this.latch=latch;
            this.list=list;
        }

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
            Integer seqno=null;
            while((seqno=list.poll()) != null) {
                Integer el=table.remove(seqno);
                if(el != null)
                    num_removed++;
            }
            System.out.printf("Thread %s: removed %d element(s)\n", Thread.currentThread().getId(), num_removed);
        }
    }

    protected static class CountNonNullElements implements RequestTable.Visitor<Integer> {
        protected int num;

        public boolean visit(Integer element) {
            if(element != null)
                num++;
            return true;
        }
    }

    protected static class Counter extends CountNonNullElements {
        public boolean visit(Integer element) {
            num++;
            return true;
        }
    }
}
