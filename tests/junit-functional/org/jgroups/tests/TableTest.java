package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.SeqnoList;
import org.jgroups.util.Table;
import org.jgroups.util.Tuple;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** Tests {@link org.jgroups.util.Table<Integer>}
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class TableTest {

    public static void testCreation() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        System.out.println("table = " + table);
        int size=table.size();
        assert size == 0;
        assert table.get(15) == null;
        assertIndices(table, 0, 0, 0);
    }

    public void testAdd() {
        Table<Integer> buf=new Table<Integer>(3, 10, 0);
        buf.add(1, 322649);
        buf.add(2, 100000);
        System.out.println("buf = " + buf);
        assert buf.size() == 2;
    }

    public void testAddList() {
        Table<Integer> buf=new Table<Integer>(3, 10, 0);
        List<Tuple<Long,Integer>> msgs=createList(1,2);
        boolean rc=buf.add(msgs);
        System.out.println("buf = " + buf);
        assert rc;
        assert buf.size() == 2;
    }

    public void testAddListWithRemoval() {
        Table<Integer> buf=new Table<Integer>(3, 10, 0);
        List<Tuple<Long,Integer>> msgs=createList(1,2,3,4,5,6,7,8,9,10);
        int size=msgs.size();
        boolean added=buf.add(msgs);
        System.out.println("buf = " + buf);
        assert added;
        assert msgs.size() == size;

        msgs=createList(1,3,5,7);
        size=msgs.size();
        added=buf.add(msgs, true);
        System.out.println("buf = " + buf);
        assert !added;
        assert msgs.isEmpty();

        msgs=createList(1,3,5,7,9,10,11,12,13,14,15);
        size=msgs.size();
        added=buf.add(msgs, true);
        System.out.println("buf = " + buf);
        assert added;
        assert msgs.size() == 5;
    }

    public static void testAddition() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        assert !table.add(0, 0);
        addAndGet(table,  1,5,9,10,11,19,20,29);
        System.out.println("table: " + table.dump());
        assert table.size() == 8;
        int size=table.computeSize();
        assert size == 8;
        assert table.size() == table.computeSize();
        assertCapacity(table.capacity(), 3, 10);
        assertIndices(table, 0, 0, 29);
    }




    public static void testAdditionList() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        List<Tuple<Long,Integer>> msgs=createList(0);
        assert !table.add(msgs);
        long[] seqnos={1,5,9,10,11,19,20,29};
        msgs=createList(seqnos);
        assert table.add(msgs);
        System.out.println("table: " + table.dump());
        for(long seqno: seqnos)
            assert table.get(seqno) == seqno;
        assert table.size() == 8;
        int size=table.computeSize();
        assert size == 8;
        assert table.size() == table.computeSize();
        assertCapacity(table.capacity(), 3, 10);
        assertIndices(table, 0, 0, 29);
    }

    public static void testAdditionWithOffset() {
        Table<Integer> table=new Table<Integer>(3, 10, 100);
        addAndGet(table, 101,105,109,110,111,119,120,129);
        System.out.println("table: " + table.dump());
        assert table.size() == 8;
        assertCapacity(table.capacity(), 3, 10);
        assertIndices(table, 100, 100, 129);
    }


    public static void testAdditionListWithOffset() {
        Table<Integer> table=new Table<Integer>(3, 10, 100);
        long seqnos[]={101,105,109,110,111,119,120,129};
        List<Tuple<Long,Integer>> msgs=createList(seqnos);
        System.out.println("table: " + table.dump());
        assert table.add(msgs);
        assert table.size() == 8;
        for(long seqno: seqnos)
            assert table.get(seqno) == seqno;
        assertCapacity(table.capacity(), 3, 10);
        assertIndices(table, 100, 100, 129);
    }



    public static void testAdditionWithOffset2() {
        Table<Integer> table=new Table<Integer>(3, 10, 2);
        addAndGet(table, 1000,1001);
        table.compact();
        addAndGet(table, 1005,1009,1010,1011,1019,1020,1029);
        System.out.println("table: " + table.dump());
        assert table.size() == 9;
        assertIndices(table, 2, 2, 1029);
    }

    public void testAddWithWrapAround() {
        Table<Integer> buf=new Table<Integer>(3, 10, 5);
        for(int i=6; i <=15; i++)
            assert buf.add(i, i) : "addition of seqno " + i + " failed";
        System.out.println("buf = " + buf);
        for(int i=0; i < 3; i++) {
            Integer val=buf.remove(false);
            System.out.println("removed " + val);
            assert val != null;
        }
        System.out.println("buf = " + buf);

        long low=buf.getLow();
        buf.purge(8);
        System.out.println("buf = " + buf);
        assert buf.getLow() == 8;
        for(long i=low; i <= 8; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";

        for(int i=16; i <= 18; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);

        while(buf.remove(false) != null)
            ;
        System.out.println("buf = " + buf);
        assert buf.isEmpty();
        assert buf.getNumMissing() == 0;
        low=buf.getLow();
        buf.purge(18);
        assert buf.getLow() == 18;
        for(long i=low; i <= 18; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }


    public void testAddWithWrapAroundAndRemoveMany() {
        Table<Integer> buf=new Table<Integer>(3, 10, 5);
        for(int i=6; i <=15; i++) {
            boolean rc=buf.add(i, i);
            assert rc : "addition of seqno " + i + " failed";
        }
        System.out.println("buf = " + buf);
        List<Integer> removed=buf.removeMany(true, 3);
        System.out.println("removed " + removed);
        System.out.println("buf = " + buf);
        for(int i: removed)
            assert buf._get(i) == null;
        assertIndices(buf, 8, 8, 15);

        for(int i=16; i <= 18; i++)
            assert buf.add(i, i);
        System.out.println("buf = " + buf);

        removed=buf.removeMany(true, 0);
        System.out.println("buf = " + buf);
        System.out.println("removed = " + removed);
        assert removed.size() == 10;
        for(int i: removed)
            assert buf._get(i) == null;

        assert buf.isEmpty();
        assert buf.getNumMissing() == 0;
        assertIndices(buf, 18, 18, 18);
    }

    public void testAddMissing() {
        Table<Integer> buf=new Table<Integer>(3, 10, 0);
        for(int i: Arrays.asList(1,2,4,5,6))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        assert buf.size() == 5 && buf.getNumMissing() == 1;

        Integer num=buf.remove();
        assert num == 1;
        num=buf.remove();
        assert num == 2;
        num=buf.remove();
        assert num == null;

        buf.add(3, 3);
        System.out.println("buf = " + buf);
        assert buf.size() == 4 && buf.getNumMissing() == 0;

        for(int i=3; i <= 6; i++) {
            num=buf.remove();
            System.out.println("buf = " + buf);
            assert num == i;
        }

        num=buf.remove();
        assert num == null;
    }
    
    
    public static void testDuplicateAddition() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        addAndGet(table,  1,5,9,10);
        assert !table.add(5,5);
        assert table.get(5) == 5;
        assert table.size() == 4;
        assertIndices(table, 0, 0, 10);
    }

    public void testAddWithInvalidSeqno() {
        Table<Integer> buf=new Table<Integer>(3, 10, 20);
        boolean success=buf.add(10, 0);
        assert !success;

        success=buf.add(20, 0);
        assert !success;
        assert buf.isEmpty();
    }

    /**
     * Runs NUM adder threads, each adder adds 1 (unique) seqno. When all adders are done, we should have
     * NUM elements in the table.
     */
    public void testConcurrentAdd() {
        final int NUM=100;
        final Table<Integer> buf=new Table<Integer>(3, 10, 0);

        CountDownLatch latch=new CountDownLatch(1);
        Adder[] adders=new Adder[NUM];
        for(int i=0; i < adders.length; i++) {
            adders[i]=new Adder(latch, i+1, buf);
            adders[i].start();
        }

        System.out.println("starting threads");
        latch.countDown();
        System.out.print("waiting for threads to be done: ");
        for(Adder adder: adders) {
            try {
                adder.join();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("OK");
        System.out.println("buf = " + buf);
        assert buf.size() == NUM;
    }


    /**
        * Creates a table and fills it to capacity. Then starts a number of adder threads, each trying to add a
        * seqno, blocking until there is more space. Each adder will block until the remover removes elements, so the
        * adder threads get unblocked and can then add their elements to the buffer.
        */
       public void testConcurrentAddAndRemove() throws Exception {
           final int NUM=5;
           final Table<Integer> buf=new Table<Integer>(3, 10, 0);
           for(int i=1; i <= 10; i++)
               buf.add(i, i); // fill the buffer, add() will block now

           CountDownLatch latch=new CountDownLatch(1);
           Adder[] adders=new Adder[NUM];
           for(int i=0; i < adders.length; i++) {
               adders[i]=new Adder(latch, i+11, buf);
               adders[i].start();
           }

           System.out.println("releasing threads");
           latch.countDown();
           System.out.print("waiting for threads to be done: ");

           Thread remover=new Thread("Remover") {
               public void run() {
                   Util.sleep(2000);
                   List<Integer> list=buf.removeMany(true, 5);
                   System.out.println("\nremover: removed = " + list);
               }
           };
           remover.start();

           for(Adder adder: adders) {
               try {
                   adder.join();
               }
               catch(InterruptedException e) {
                   e.printStackTrace();
               }
           }

           remover.join();

           System.out.println("OK");
           System.out.println("buf = " + buf);
           assert buf.size() == 10;
           assertIndices(buf, 5, 5, 15);

           List<Integer> list=buf.removeMany(true, 0);
           System.out.println("removed = " + list);
           assert list.size() == 10;
           for(int i=6; i <=15; i++)
               assert list.contains(i);
           assertIndices(buf, 15, 15, 15);
       }



    public void testIndex() {
        Table<Integer> buf=new Table<Integer>(3, 10, 5);
        assert buf.getHighestDelivered() == 5;
        assert buf.getHighestReceived() == 5;
        buf.add(6,6); buf.add(7,7);
        buf.remove(false); buf.remove(false);
        long low=buf.getLow();
        assert low == 5;
        buf.purge(4);
        buf.purge(5);
        buf.purge(6);
        buf.purge(7);
        System.out.println("buf = " + buf);
        for(long i=low; i <= 7; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }


    public void testIndexWithRemoveMany() {
        Table<Integer> buf=new Table<Integer>(3, 10, 5);
        assert buf.getHighestDelivered() == 5;
        assert buf.getHighestReceived() == 5;
        buf.add(6,6); buf.add(7,7);
        long low=buf.getLow();
        buf.removeMany(true,0);
        System.out.println("buf = " + buf);
        for(long i=low; i <= 7; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
        assertIndices(buf, 7, 7, 7);
    }


    public static void testComputeSize() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int num: Arrays.asList(1,2,3,4,5,6,7,8,9,10))
            table.add(num, num);
        System.out.println("table = " + table);
        assert table.computeSize() == 10;
        table.removeMany(false, 3);
        System.out.println("table = " + table);
        assert table.computeSize() == 7;

        table.removeMany(true, 4);
        System.out.println("table = " + table);
        assert table.computeSize() == table.size();
        assert table.computeSize() == 3;
    }

    public static void testComputeSize2() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        table.add(1, 1);
        System.out.println("table = " + table);
        assert table.computeSize() == table.size();
        assert table.computeSize() == 1;
        table.remove(false);
        System.out.println("table = " + table);
        assert table.computeSize() == table.size();
        assert table.computeSize() == 0;
    }

    public static void testRemove() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 9; i++)
            table.add(i,i);
        table.add(20, 20);
        System.out.println("table = " + table);
        assert table.size() == 10;
        assertIndices(table, 0, 0, 20);

        int num_null_msgs=table.getNumMissing();
        System.out.println("num_null_msgs = " + num_null_msgs);
        assert num_null_msgs == 10;

        for(long i=1; i <= 10; i++) // 10 is missing
            table.remove();
        System.out.println("table = " + table);
        assert table.size() == 1;
        assertIndices(table, 9, 9, 20);

        num_null_msgs=table.getNumMissing();
        System.out.println("num_null_msgs = " + num_null_msgs);
        assert num_null_msgs == 10;
    }

    public void testRemove2() {
        final Table<Integer> buf=new Table<Integer>(3, 10, 0);
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        assertIndices(buf, 0, 0, 5);

        Integer el=buf.remove(true);
        System.out.println("el = " + el);
        assert el.equals(1);

        el=buf.remove(false);  // not encouraged ! nullify should always be true or false
        System.out.println("el = " + el);
        assert el.equals(2);

        el=buf.remove(true);
        System.out.println("el = " + el);
        assert el.equals(3);
    }



    public static void testRemoveMany() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int seqno: Arrays.asList(1,2,3,4,5,7,8,9,10))
            table.add(seqno, seqno);
        System.out.println("table = " + table);
        assertIndices(table, 0, 0, 10);
        List<Integer> list=table.removeMany(true,4);
        System.out.println("list=" + list + ", table=" + table);
        assert table.size() == 5 && table.getNumMissing() == 1;
        assert list != null && list.size() == 4;
        for(int num: Arrays.asList(1,2,3,4))
            assert list.contains(num);
        assertIndices(table, 4, 4, 10);
    }

    public void testRemoveMany2() {
        Table<Integer> buf=new Table<Integer>(3, 10, 0);
        for(int i: Arrays.asList(1,2,3,4,5,6,7,9,10))
            buf.add(i, i);
        List<Integer> list=buf.removeMany(false,3);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;

        list=buf.removeMany(false, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 4;

        list=buf.removeMany(false, 10);
        assert list == null;

        buf.add(8, 8);
        list=buf.removeMany(false, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
    }

    public void testRemoveManyWithNulling() {
        Table<Integer> buf=new Table<Integer>(3, 10, 0);
        for(int i: Arrays.asList(1,2,3,4,5,6,7,9,10))
            buf.add(i, i);
        List<Integer> list=buf.removeMany(true, 3);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        for(int i: list)
            assert buf._get(i) == null;

        list=buf.removeMany(true, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 4;
        for(int i: list)
            assert buf._get(i) == null;

        list=buf.removeMany(false, 10);
        assert list == null;

        buf.add(8, 8);
        list=buf.removeMany(true, 0);
        System.out.println("list = " + list);
        assert list != null && list.size() == 3;
        for(int i: list)
            assert buf._get(i) == null;
    }


    public static void testRemoveManyWithWrapping() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int seqno: Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,15,16,17,18,19,20))
            table.add(seqno, seqno);
        System.out.println("table = " + table);
        assertIndices(table, 0, 0, 20);
        assert table.size() == 18 && table.getNumMissing() == 2;
        List<Integer> list=table.removeMany(true, 0);
        assert list.size() == 12;
        assertIndices(table, 12, 12, 20);
        assert table.size() == 6 && table.getNumMissing() == 2;
        table.purge(12);
        assertIndices(table, 12, 12, 20);
        assert table.size() == 6 && table.getNumMissing() == 2;
    }

    public static void testRemoveManyWithWrapping2() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int seqno: Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,15,16,17,18,19,20))
            table.add(seqno, seqno);
        System.out.println("table = " + table);
        assertIndices(table, 0, 0, 20);
        assert table.size() == 18 && table.getNumMissing() == 2;
        List<Integer> list=table.removeMany(false,0);
        assert list.size() == 12;
        assertIndices(table, 0, 12, 20);
        assert table.size() == 6 && table.getNumMissing() == 2;
        table.purge(12);
        assertIndices(table, 12, 12, 20);
        assert table.size() == 6 && table.getNumMissing() == 2;
    }


    public static void testForEach() {
        class MyVisitor<T> implements Table.Visitor<T> {
            List<int[]> list=new ArrayList<int[]>(20);
            public boolean visit(long seqno, T element, int row, int column) {
                System.out.println("#" + seqno + ": " + element + ", row=" + row + ", column=" + column);
                list.add(new int[]{row,column});
                return true;
            }
        }
        MyVisitor<Integer> visitor=new MyVisitor<Integer>();

        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <=20; i++)
           table.add(i, i);
        System.out.println("table = " + table);
        table.forEach(table.getLow()+1, table.getHighestReceived()-1, visitor);
        int count=1;
        for(int[] pair: visitor.list) {
            int row=pair[0], column=pair[1];
            if(count < Util.getNextHigherPowerOfTwo(10)) {
                assert row == 0;
                assert column == count;
            }
            else {
                assert row == 1;
                assert column == count - Util.getNextHigherPowerOfTwo(10);
            }
            count++;
        }
    }

    public void testGet() {
        final Table<Integer> buf=new Table<Integer>(3, 10, 0);
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
        assert buf.get(0) == null;
        assert buf.get(1) == 1;
        assert buf.get(10) == null;
        assert buf.get(5) == 5;
        assert buf.get(6) == null;
    }

    public void testGetList() {
        final Table<Integer> buf=new Table<Integer>(3, 10, 0);
        for(int i: Arrays.asList(1,2,3,4,5))
            buf.add(i, i);
        List<Integer> elements=buf.get(3,5);
        System.out.println("elements = " + elements);
        assert elements != null && elements.size() == 3;
        assert elements.contains(3) && elements.contains(4) && elements.contains(5);

        elements=buf.get(4, 10);
        System.out.println("elements = " + elements);
        assert elements != null && elements.size() == 2;
        assert elements.contains(4) && elements.contains(5);

        elements=buf.get(10, 20);
        assert elements == null;
    }


    public static void testGetNullMessages() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        table.add(1, 1);
        table.add(100, 100);
        System.out.println("table = " + table);
        int num_null_elements=table.getNumMissing();
        assert num_null_elements == 98; // [2 .. 99]

        table.add(50,50);
        System.out.println("table = " + table);
        assert table.size() == 3;
        assert table.getNumMissing() == 97;
    }

    public static void testGetNullMessages2() {
        Table<Integer> table=new Table<Integer>(1, 10, 0);
        table.add(1, 1);
        table.add(5, 5);
        System.out.println("table = " + table);
        int num_null_elements=table.getNumMissing();
        assert num_null_elements == 3; // [2 .. 4]

        table.add(10,10);
        System.out.println("table = " + table);
        assert table.size() == 3;
        assert table.getNumMissing() == 7;

        table.add(14,14);
        System.out.println("table = " + table);
        assert table.size() == 4;
        assert table.getNumMissing() == 10;

        while(table.remove() != null)
            ;
        System.out.println("table = " + table);
        assert table.size() == 3;
        assert table.getNumMissing() == 10;
    }

    public static void testGetMissing() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int num: Arrays.asList(2,4,6,8))
            table.add(num, num);
        System.out.println("table = " + table);
        SeqnoList missing=table.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 4;
        assert table.getNumMissing() == 4;
    }

    public static void testGetMissing2() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int num: Arrays.asList(3,4,5))
            table.add(num, num);
        System.out.println("table = " + table);
        SeqnoList missing=table.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 2; // the range [1-2]
        assert table.getNumMissing() == 2;
    }

    public static void testGetMissing3() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int num: Arrays.asList(8))
            table.add(num, num);
        System.out.println("table = " + table);
        SeqnoList missing=table.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 7;
        assert table.getNumMissing() == 7;
    }

    public void testGetMissing4() {
        Table<Integer> buf=new Table<Integer>(3, 30, 0);
        for(int i: Arrays.asList(2,5,10,11,12,13,15,20,28,30))
            buf.add(i, i);
        System.out.println("buf = " + buf);
        int missing=buf.getNumMissing();
        assert missing == 20;
        System.out.println("missing=" + missing);
        SeqnoList missing_list=buf.getMissing();
        System.out.println("missing_list = " + missing_list);
        assert missing_list.size() == missing;
    }

    public void testGetMissing5() {
        Table<Integer> buf=new Table<Integer>(3, 10, 0);
        buf.add(1,1);
        SeqnoList missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert missing == null && buf.getNumMissing() == 0;

        buf=new Table<Integer>(3, 10, 0);
        buf.add(10,10);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.getNumMissing() == missing.size();

        buf=new Table<Integer>(3, 10, 0);
        buf.add(5,5);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert buf.getNumMissing() == missing.size();

        buf=new Table<Integer>(3, 10, 0);
        buf.add(5,5); buf.add(7,7);
        missing=buf.getMissing();
        System.out.println("missing = " + missing);
        assert missing.size() == 5;
        assert buf.getNumMissing() == missing.size();
    }


    public static void testGetMissingLast() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int num: Arrays.asList(1,2,3,4,5,6,8))
            table.add(num, num);
        System.out.println("table = " + table);
        SeqnoList missing=table.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 1;
        assert table.getNumMissing() == 1;
    }

    public static void testGetMissingFirst() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int num: Arrays.asList(2,3,4,5))
            table.add(num, num);
        System.out.println("table = " + table);
        SeqnoList missing=table.getMissing();
        System.out.println("missing=" + missing);
        assert missing.size() == 1;
        assert table.getNumMissing() == 1;
    }


    public void testGetHighestDeliverable() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        System.out.println("table = " + table);
        long highest_deliverable=table.getHighestDeliverable(), hd=table.getHighestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 0;
        assert highest_deliverable == 0;

        for(int num: Arrays.asList(1,2,3,4,5,6,8))
            table.add(num, num);
        System.out.println("table = " + table);
        highest_deliverable=table.getHighestDeliverable();
        hd=table.getHighestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 0;
        assert highest_deliverable == 6;

        table.removeMany(true, 4);
        System.out.println("table = " + table);
        highest_deliverable=table.getHighestDeliverable();
        hd=table.getHighestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 4;
        assert highest_deliverable == 6;

        table.removeMany(true, 100);
        System.out.println("table = " + table);
        highest_deliverable=table.getHighestDeliverable();
        hd=table.getHighestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 6;
        assert highest_deliverable == 6;

        table.add(7,7);
        System.out.println("table = " + table);
        highest_deliverable=table.getHighestDeliverable();
        hd=table.getHighestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 6;
        assert highest_deliverable == 8;

        table.removeMany(true, 100);
        System.out.println("table = " + table);
        highest_deliverable=table.getHighestDeliverable();
        hd=table.getHighestDelivered();
        System.out.println("highest delivered=" + hd + ", highest deliverable=" + highest_deliverable);
        assert hd == 8;
        assert highest_deliverable == 8;
    }

    public static void testMassAddition() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        final int NUM_ELEMENTS=10005;
        for(int i=1; i <= NUM_ELEMENTS; i++)
            table.add(i, i);
        System.out.println("table = " + table);
        assert table.size() == NUM_ELEMENTS;
        assertCapacity(table.capacity(), table.getNumRows(), 10);
        assertIndices(table, 0, 0, NUM_ELEMENTS);
        assert table.getNumMissing() == 0;
    }

    public static void testResize() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        assertCapacity(table.capacity(), table.getNumRows(), 10);
        addAndGet(table, 30);
        addAndGet(table, 35);
        assertCapacity(table.capacity(), table.getNumRows(), 10);
        addAndGet(table, 500);
        assertCapacity(table.capacity(), table.getNumRows(), 10);

        addAndGet(table, 515);
        assertCapacity(table.capacity(), table.getNumRows(), 10);
    }

    public void testResizeWithPurge() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 100; i++)
            addAndGet(table, i);
        System.out.println("table: " + table);
        
        // now remove 60 messages
        for(int i=1; i <= 60; i++) {
            Integer num=table.remove();
            assert num != null && num == i;
        }
        System.out.println("table after removal of seqno 60: " + table);

        table.purge(50);
        System.out.println("now triggering a resize() by addition of seqno=120");
        addAndGet(table, 120);
        
    }


    public void testResizeWithPurgeAndGetOfNonExistingElement() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 50; i++)
            addAndGet(table, i);
        System.out.println("table: " + table);
        assertIndices(table, 0, 0, 50);
        assert table.size() == 50 && table.getNumMissing() == 0;

        // now remove 15 messages
        for(long i=1; i <= 15; i++) {
            Integer num=table.remove(false);
            assert num != null && num == i;
        }
        System.out.println("table after removal of seqno 15: " + table);
        assertIndices(table, 0, 15, 50);
        assert table.size() == 35 && table.getNumMissing() == 0;

        table.purge(15);
        System.out.println("now triggering a resize() by addition of seqno=55");
        addAndGet(table, 55);
        assertIndices(table, 15, 15, 55);
        assert table.size() == 36 && table.getNumMissing() == 4;

        // now we have elements 40-49 in row 1 and 55 in row 2:
        List<Integer> list=new ArrayList<Integer>(20);
        for(int i=16; i < 50; i++)
            list.add(i);
        list.add(55);

        for(long i=table.getOffset(); i < table.capacity() + table.getOffset(); i++) {
            Integer num=table._get(i);
            if(num != null) {
                System.out.println("num=" + num);
                list.remove(num);
            }
        }

        System.out.println("table:\n" + table.dump());
        assert list.isEmpty() : " list: " + Util.print(list);
    }


    public void testResizeWithPurge2() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 50; i++)
            addAndGet(table, i);
        System.out.println("table = " + table);
        assert table.size() == 50;
        assertCapacity(table.capacity(), table.getNumRows(), 10);
        assertIndices(table, 0, 0, 50);
        table.removeMany(false, 43);
        System.out.println("table = " + table);
        assertIndices(table, 0, 43, 50);
        table.purge(43);
        System.out.println("table = " + table);
        assertIndices(table, 43, 43, 50);
        addAndGet(table, 52);
        assert table.get(43) == null;

        for(long i=44; i <= 50; i++) {
            Integer num=table.get(i);
            assert num != null && num == i;
        }

        assert table.get(50) != null;
        assert table.get(51) == null;
        Integer num=table.get(52);
        assert num != null && num == 52;
        assert table.get(53) == null;
    }


    public static void testMove() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i < 50; i++)
            addAndGet(table, i);
        table.removeMany(true, 49);
        assert table.isEmpty();
        addAndGet(table, 50);
        assert table.size() == 1;
        assertCapacity(table.capacity(), table.getNumRows(), 10);
    }

    public static void testMove2() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i < 30; i++)
            table.add(i, i);
        table.removeMany(true, 23);
        System.out.println("table = " + table);
        table.add(35, 35); // triggers a resize() --> move()
        for(int i=1; i <= 23; i++)
            assert table._get(i) == null;
        for(int i=24; i < 30; i++)
            assert table._get(i) != null;
    }

    public static void testMove3() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i < 30; i++)
            table.add(i, i);
        table.removeMany(true, 23);
        System.out.println("table = " + table);
        table.add(30, 30); // triggers a resize() --> move()
        for(int i=1; i <= 23; i++)
            assert table._get(i) == null;
        for(int i=24; i < 30; i++)
            assert table._get(i) != null;
    }


    public static void testPurge() {
        Table<Integer> table=new Table<Integer>(5, 10, 0);
        for(int seqno=1; seqno <= 25; seqno++)
            table.add(seqno, seqno);

        int[] seqnos={30,31,32,37,38,39,40,41,42,47,48,49};
        for(int seqno: seqnos)
            table.add(seqno, seqno);

        System.out.println("table (before remove):\n" + table.dump());
        for(int seqno=1; seqno <= 22; seqno++)
            table.remove(false);

        System.out.println("\ntable (after remove 22, before purge):\n" + table.dump());
        table.purge(22);
        System.out.println("\ntable: (after purge 22):\n" + table.dump());
        assert table.size() == 3 + seqnos.length;
        assert table.computeSize() == table.size();
    }

    public void testPurge2() {
        Table<Integer> buf=new Table<Integer>(3, 10, 0);
        for(int i=1; i <=7; i++) {
            buf.add(i, i);
            buf.remove(false);
        }
        System.out.println("buf = " + buf);
        assert buf.isEmpty();
        long low=buf.getLow();
        buf.purge(3);
        assert buf.getLow() == 3;
        for(long i=low; i <= 3; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";


        buf.purge(6);
        assert buf._get(6) == null;
        buf.purge(7);
        assert buf._get(7) == null;
        assert buf.getLow() == 7;
        assert buf.isEmpty();

        for(int i=7; i <= 14; i++) {
            buf.add(i, i);
            buf.remove(false);
        }

        System.out.println("buf = " + buf);
        assert buf.isEmpty();

        low=buf.getLow(); assert low == 7;
        buf.purge(12);
        System.out.println("buf = " + buf);
        assert buf.getLow() == 12;
        for(long i=low; i <= 12; i++)
            assert buf._get(i) == null : "message with seqno=" + i + " is not null";
    }


    public void testPurge3() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 100; i++)
            table.add(i, i);
        System.out.println("table = " + table);
        table.removeMany(true, 53);
        for(int i=54; i <= 100; i++)
            assert table.get(i) == i;
    }

    public void testPurge4() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 100; i++)
            table.add(i, i);
        System.out.println("table = " + table);
        table.removeMany(false, 53);
        table.purge(53);
        for(int i=54; i <= 100; i++)
            assert table.get(i) == i;
    }

    public void testPurge5() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 100; i++)
            table.add(i, i);
        System.out.println("table = " + table);
        table.removeMany(false, 0);

        table.purge(10);
        for(int i=1; i <= 10; i++)
            assert table._get(i) == null;
        for(int i=11; i <= 100; i++)
            assert table.get(i) == i;

        table.purge(10);
        for(int i=11; i <= 100; i++)
            assert table.get(i) == i;

        table.purge(50);
        for(int i=1; i <= 50; i++)
            assert table._get(i) == null;
        for(int i=51; i <= 100; i++)
            assert table.get(i) == i;

        table.purge(100);
        for(int i=51; i <= 100; i++)
            assert table._get(i) == null;
    }


    public void testPurgeForce() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 30; i++)
            table.add(i, i);
        System.out.println("table = " + table);
        table.purge(15, true);
        System.out.println("table = " + table);
        assertIndices(table, 15, 15, 30);
        for(int i=1; i <= 15; i++)
            assert table._get(i) == null;
        for(int i=16; i<= 30; i++)
            assert table._get(i) != null;
        assert table.get(5) == null && table.get(25) != null;

        table.purge(30, true);
        System.out.println("table = " + table);
        assertIndices(table, 30, 30, 30);
        assert table.isEmpty();
        for(int i=1; i <= 30; i++)
            assert table._get(i) == null;

        for(int i=31; i <= 40; i++)
            table.add(i, i);
        System.out.println("table = " + table);
        assert table.size() == 10;
        assertIndices(table, 30, 30, 40);

        table.purge(50, true);
        System.out.println("table = " + table);
        assert table.isEmpty();
        assertIndices(table, 40, 40, 40);
    }

    // Tests purge(40) followed by purge(20) - the second purge() should be ignored
    // https://issues.jboss.org/browse/JGRP-1872
    public void testPurgeLower() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 30; i++)
            table.add(i, i);
        System.out.println("table = " + table);
        table.purge(20, true);
        assertIndices(table, 20, 20, 30);

        table.purge(15, true);
        assertIndices(table, 20,20, 30);
    }


    public void testCompact() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 80; i++)
            addAndGet(table, i);
        assert table.size() == 80;
        assertIndices(table, 0, 0, 80);
        List<Integer> list=table.removeMany(false,60);
        assert list.size() == 60;
        assert list.get(0) == 1 && list.get(list.size() -1) == 60;
        assertIndices(table, 0, 60, 80);
        table.purge(60);
        assertIndices(table, 60, 60, 80);
        assert table.size() == 20;
        table.compact();
        assertIndices(table, 60, 60, 80);
        assert table.size() == 20;
        assertCapacity(table.capacity(), table.getNumRows(), 10);
    }


    public void testCompact2() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 80; i++)
             addAndGet(table, i);
        assert table.size() == 80;
        for(long i=1; i <= 60; i++)
            table.remove();

        assert table.size() == 20;
        table.compact();
        assert table.size() == 20;
        assertCapacity(table.capacity(), table.getNumRows(), 10);
    }


    protected static void assertCapacity(int actual_capacity, int num_rows, int elements_per_row) {
        int actual_elements_per_row=Util.getNextHigherPowerOfTwo(elements_per_row);
        int expected_capacity=num_rows * actual_elements_per_row;
        assert actual_capacity == expected_capacity
          : "expected capacity of " + expected_capacity + " but got " + actual_capacity;
    }


    protected static void addAndGet(Table<Integer> table, int ... seqnos) {
        for(int seqno: seqnos) {
            boolean added=table.add((long)seqno, seqno);
            assert added;
            Integer val=table.get(seqno);
            assert val != null && val == seqno;
        }
    }

    protected static List<Tuple<Long,Integer>> createList(long ... seqnos) {
        if(seqnos == null)
            return null;
        List<Tuple<Long,Integer>> msgs=new ArrayList<Tuple<Long,Integer>>(seqnos.length);
        for(long seqno: seqnos)
            msgs.add(new Tuple<Long,Integer>(seqno, (int)seqno));
        return msgs;
    }

    protected static <T> void assertIndices(Table<T> table, long low, long hd, long hr) {
        assert table.getLow() == low : "expected low=" + low + " but was " + table.getLow();
        assert table.getHighestDelivered() == hd : "expected hd=" + hd + " but was " + table.getHighestDelivered();
        assert table.getHighestReceived()  == hr : "expected hr=" + hr + " but was " + table.getHighestReceived();
    }

     protected static class Adder extends Thread {
        protected final CountDownLatch latch;
        protected final int            seqno;
        protected final Table<Integer> buf;

         public Adder(CountDownLatch latch, int seqno, Table<Integer> buf) {
            this.latch=latch;
            this.seqno=seqno;
            this.buf=buf;
        }

        public void run() {
            try {
                latch.await();
                Util.sleepRandom(10, 500);
                buf.add(seqno, seqno);
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
