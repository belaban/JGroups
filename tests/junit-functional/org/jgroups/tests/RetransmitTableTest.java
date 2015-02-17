package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.util.RetransmitTable;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/** Tests {@link org.jgroups.util.RetransmitTable}
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class RetransmitTableTest {
    static final Message MSG=new Message(null, null, "test");

    public static void testCreation() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        int size=table.size();
        assert size == 0;
        assert table.get(15) == null;
    }


    public static void testAddition() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        addAndGet(table,  0,  "0");
        addAndGet(table,  1,  "1");
        addAndGet(table,  5,  "5");
        addAndGet(table,  9,  "9");
        addAndGet(table, 10, "10");
        addAndGet(table, 11, "11");
        addAndGet(table, 19, "19");
        addAndGet(table, 20, "20");
        addAndGet(table, 29, "29");
        System.out.println("table: " + table.dump());
        assert table.size() == 9;
        assert table.size() == table.computeSize();
        assertCapacity(table.capacity(), 3, 10);
    }


    public static void testAdditionWithOffset() {
        RetransmitTable table=new RetransmitTable(3, 10, 100);
        addAndGet(table, 100, "100");
        addAndGet(table, 101, "101");
        addAndGet(table, 105, "105");
        addAndGet(table, 109, "109");
        addAndGet(table, 110, "110");
        addAndGet(table, 111, "111");
        addAndGet(table, 119, "119");
        addAndGet(table, 120, "120");
        addAndGet(table, 129, "129");
        System.out.println("table: " + table.dump());
        assert table.size() == 9;
        assertCapacity(table.capacity(), 3, 10);
    }

    public static void testAdditionWithOffset2() {
        RetransmitTable table=new RetransmitTable(3, 10, 2);
        addAndGet(table, 1000, "1000");
        addAndGet(table, 1001, "1001");
        table.compact();
        addAndGet(table, 1005, "1005");
        addAndGet(table, 1009, "1009");
        addAndGet(table, 1010, "1010");
        addAndGet(table, 1011, "1011");
        addAndGet(table, 1019, "1019");
        addAndGet(table, 1020, "1020");
        addAndGet(table, 1029, "1029");
        System.out.println("table: " + table.dump());
        assert table.size() == 9;
    }
    

    public static void testDuplicateAddition() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        addAndGet(table,  0,  "0");
        addAndGet(table,  1,  "1");
        addAndGet(table,  5,  "5");
        addAndGet(table,  9,  "9");
        addAndGet(table, 10, "10");

        assert !table.put(5, new Message());
        assert table.get(5).getObject().equals("5");
        assert table.size() == 5;
    }


    public static void testRemove() {
        RetransmitTable table=new RetransmitTable(3, 10, 1);
        for(long i=1; i <= 20; i++)
            if(i % 2 == 0)
                table.put(i, new Message());
        System.out.println("table = " + table);
        assert table.size() == 10;

        int num_null_msgs=table.getNullMessages(0, 20);
        System.out.println("num_null_msgs = " + num_null_msgs);
        assert num_null_msgs == 10;

        for(long i=1; i <= 10; i++)
            table.remove(i);
        System.out.println("table = " + table);
        assert table.size() == 5;

        num_null_msgs=table.getNullMessages(0, 20);
        System.out.println("num_null_msgs = " + num_null_msgs);
        assert num_null_msgs == 15;
    }

    public static void testGetNullMessages() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        table.put(1, MSG);
        table.put(100, MSG);
        System.out.println("table = " + table);
        int num_null_elements=table.getNullMessages(0, 100);
        assert num_null_elements == 98; // [1 .. 99] excluding 100, as it has been received
    }

    public static void testDumpMatrix() {
        RetransmitTable table=new RetransmitTable(3, 10, 1);
        long[] seqnos={1,3,5,7,9,12,14,16,18,20,21,22,23,24};
        for(long seqno: seqnos)
            table.put(seqno, MSG);
        System.out.println("matrix:\n" + table.dumpMatrix());
    }


    public static void testMassAddition() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        final int NUM_MSGS=10005;
        final Message MSG=new Message(null, null, "hello world");
        for(int i=0; i < NUM_MSGS; i++)
            table.put(i, MSG);
        System.out.println("table = " + table);
        assert table.size() == NUM_MSGS;
        assertCapacity(table.capacity(), table.getLength(), 10);
    }

    public static void testResize() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        assertCapacity(table.capacity(), table.getLength(), 10);
        addAndGet(table, 30, "30");
        addAndGet(table, 35, "35");
        assertCapacity(table.capacity(), table.getLength(), 10);
        addAndGet(table, 500, "500");
        assertCapacity(table.capacity(), table.getLength(), 10);

        addAndGet(table, 515, "515");
        assertCapacity(table.capacity(), table.getLength(), 10);
    }

    public void testResizeWithPurge() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        for(long i=1; i <= 100; i++)
            addAndGet(table, i, "hello-" + i);
        System.out.println("table: " + table);
        
        // now remove 60 messages
        for(long i=1; i <= 60; i++) {
            Message msg=table.remove(i);
            assert msg.getObject().equals("hello-" + i);
        }
        System.out.println("table after removal of seqno 60: " + table);

        table.purge(50);
        System.out.println("now triggering a resize() by addition of seqno=120");
        addAndGet(table, 120, "120");
        
    }


    public void testResizeWithPurgeAndGetOfNonExistingElement() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        for(long i=0; i < 50; i++)
            addAndGet(table, i, "hello-" + i);
        System.out.println("table: " + table);

        // now remove 15 messages
        for(long i=0; i <= 15; i++) {
            Message msg=table.remove(i);
            assert msg.getObject().equals("hello-" + i);
        }
        System.out.println("table after removal of seqno 15: " + table);

        table.purge(15);
        System.out.println("now triggering a resize() by addition of seqno=55");
        addAndGet(table, 55, "hello-55");

        // now we have elements 40-49 in row 1 and 55 in row 2:
        List<String> list=new ArrayList<>(20);
        for(int i=16; i < 50; i++)
            list.add("hello-" + i);
        list.add("hello-55");

        for(long i=table.getOffset(); i < table.capacity() + table.getOffset(); i++) {
            Message msg=table.get(i);
            if(msg != null) {
                String message=(String)msg.getObject();
                System.out.println(i + ": " + message);
                list.remove(message);
            }
        }

        System.out.println("table:\n" + table.dumpMatrix());
        assert list.isEmpty() : " list: " + Util.print(list);
    }


    public void testResizeWithPurge2() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        for(long i=0; i < 50; i++)
            addAndGet(table, i, "hello-" + i);
        System.out.println("table = " + table);
        assert table.size() == 50;
        assertCapacity(table.capacity(), table.getLength(), 10);
        assert table.getHighestPurged() == 0;
        assert table.getHighest() == 49;

        table.purge(43);
        addAndGet(table, 52, "hello-52");
        assert table.get(43) == null;

        for(long i=44; i < 50; i++) {
            Message msg=table.get(i);
            assert msg != null && msg.getObject().equals("hello-" + i);
        }

        assert table.get(50) == null;
        assert table.get(51) == null;
        Message msg=table.get(52);
        assert msg != null && msg.getObject().equals("hello-52");
        assert table.get(53) == null;
    }


    public static void testMove() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        for(long i=0; i < 50; i++)
            addAndGet(table, i, "hello-" + i);
        table.purge(49);
        assert table.isEmpty();
        addAndGet(table, 50, "50");
        assert table.size() == 1;
        assertCapacity(table.capacity(), table.getLength(), 10);
    }


    public static void testPurge() {
        RetransmitTable table=new RetransmitTable(5, 10, 0);
        for(long seqno=0; seqno < 25; seqno++)
            table.put(seqno, MSG);

        long[] seqnos={30,31,32,37,38,39, 40,41,42,47,48,49};
        for(long seqno: seqnos)
            table.put(seqno, MSG);

        System.out.println("table (before remove):\n" + table.dump());
        for(long seqno=0; seqno <= 22; seqno++)
            table.remove(seqno);

        System.out.println("\ntable (after remove 22, before purge):\n" + table.dump());
        table.purge(22);
        System.out.println("\ntable: (after purge 22):\n" + table.dump());
        assert table.size() == 2 + seqnos.length;
    }


    public void testCompact() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        for(long i=0; i < 80; i++)
            addAndGet(table, i, "hello-" + i);
        assert table.size() == 80;
        table.purge(59);
        assert table.size() == 20;
        table.compact();
        assert table.size() == 20;
        assertCapacity(table.capacity(), table.getLength(), 10);
    }


    public void testCompactWithAutomaticPurging() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        table.setAutomaticPurging(true);
        for(long i=0; i < 80; i++)
             addAndGet(table, i, "hello-" + i);
        assert table.size() == 80;
        for(long i=0; i <= 59; i++)
            table.remove(i);

        assert table.size() == 20;
        table.compact();
        assert table.size() == 20;
        assertCapacity(table.capacity(), table.getLength(), 10);
    }

    public void testSizeOfAllMessages() {
        RetransmitTable table=new RetransmitTable(3, 10, 0);
        long size=table.sizeOfAllMessages(false);
        assert size == 0;
        size=table.sizeOfAllMessages(true);
        assert size == 0;

        byte[] buf=new byte[100];
        Message msg=new Message(null, null, buf);
        table.put(2,msg);

        size=table.sizeOfAllMessages(false);
        System.out.println("Size(): " + table.sizeOfAllMessages(true) + ", getLength(): " + size);
        assert size == 100;

        for(long i=5; i < 10; i++)
            table.put(i, new Message(null, null, buf));

        size=table.sizeOfAllMessages(false);
        System.out.println("Size(): " + table.sizeOfAllMessages(true) + ", getLength(): " + size);
        assert size == 6 * 100;
    }

    protected static void assertCapacity(int actual_capacity, int num_rows, int elements_per_row) {
        int actual_elements_per_row=Util.getNextHigherPowerOfTwo(elements_per_row);
        int expected_capacity=num_rows * actual_elements_per_row;
        assert actual_capacity == expected_capacity
          : "expected capacity of " + expected_capacity + " but got " + actual_capacity;
    }


    protected static void addAndGet(RetransmitTable table, long seqno, String message) {
        boolean added=table.put(seqno, new Message(null, null, message));
        assert added;
        Message msg=table.get(seqno);
        assert msg != null && msg.getObject().equals(message);
    }

}
