package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Table;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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


    public static void testAddition() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        assert !table.add(0, 0);
        addAndGet(table,  1);
        addAndGet(table,  5);
        addAndGet(table,  9);
        addAndGet(table, 10);
        addAndGet(table, 11);
        addAndGet(table, 19);
        addAndGet(table, 20);
        addAndGet(table, 29);
        System.out.println("table: " + table.dump());
        assert table.size() == 8;
        assert table.size() == table.computeSize();
        assert table.capacity() == 30;
        assertIndices(table, 0, 0, 29);
    }


    public static void testAdditionWithOffset() {
        Table<Integer> table=new Table<Integer>(3, 10, 100);
        addAndGet(table, 101);
        addAndGet(table, 105);
        addAndGet(table, 109);
        addAndGet(table, 110);
        addAndGet(table, 111);
        addAndGet(table, 119);
        addAndGet(table, 120);
        addAndGet(table, 129);
        System.out.println("table: " + table.dump());
        assert table.size() == 8;
        assert table.capacity() == 30;
        assertIndices(table, 100, 100, 129);
    }



    public static void testAdditionWithOffset2() {
        Table<Integer> table=new Table<Integer>(3, 10, 2);
        addAndGet(table, 1000);
        addAndGet(table, 1001);
        table.compact();
        addAndGet(table, 1005);
        addAndGet(table, 1009);
        addAndGet(table, 1010);
        addAndGet(table, 1011);
        addAndGet(table, 1019);
        addAndGet(table, 1020);
        addAndGet(table, 1029);
        System.out.println("table: " + table.dump());
        assert table.size() == 9;
        assertIndices(table, 2, 2, 1029);
    }
    
    
    public static void testDuplicateAddition() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        addAndGet(table,  1);
        addAndGet(table,  5);
        addAndGet(table,  9);
        addAndGet(table, 10);

        assert !table.add(5,5);
        assert table.get(5) == 5;
        assert table.size() == 4;
        assertIndices(table, 0, 0, 10);
    }


    public static void testRemove() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 9; i++)
            table.add(i,i);
        table.add(20, 20);
        System.out.println("table = " + table);
        assert table.size() == 10;
        assertIndices(table, 0, 0, 20);

        int num_null_msgs=table.getNullElements();
        System.out.println("num_null_msgs = " + num_null_msgs);
        assert num_null_msgs == 10;

        for(long i=1; i <= 10; i++) // 10 is missing
            table.remove();
        System.out.println("table = " + table);
        assert table.size() == 1;
        assertIndices(table, 9, 9, 20);

        num_null_msgs=table.getNullElements();
        System.out.println("num_null_msgs = " + num_null_msgs);
        assert num_null_msgs == 10;
    }

    public static void testRemoveMany() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int seqno: Arrays.asList(1,2,3,4,5,7,8,9,10))
            table.add(seqno, seqno);
        System.out.println("table = " + table);
        assertIndices(table, 0, 0, 10);
        List<Integer> list=table.removeMany(true,4);
        System.out.println("list=" + list + ", table=" + table);
        assert table.size() == 5 && table.getNullElements() == 1;
        assert list != null && list.size() == 4;
        for(int num: Arrays.asList(1,2,3,4))
            assert list.contains(num);
        assertIndices(table, 4, 4, 10);
    }


    public static void testRemoveManyWithWrapping() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int seqno: Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,15,16,17,18,19,20))
            table.add(seqno, seqno);
        System.out.println("table = " + table);
        assertIndices(table, 0, 0, 20);
        assert table.size() == 18 && table.getNullElements() == 2;
        List<Integer> list=table.removeMany(true, 0);
        assert list.size() == 12;
        assertIndices(table, 12, 12, 20);
        assert table.size() == 6 && table.getNullElements() == 2;
        table.purge(12);
        assertIndices(table, 12, 12, 20);
        assert table.size() == 6 && table.getNullElements() == 2;
    }

    public static void testRemoveManyWithWrapping2() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int seqno: Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,15,16,17,18,19,20))
            table.add(seqno, seqno);
        System.out.println("table = " + table);
        assertIndices(table, 0, 0, 20);
        assert table.size() == 18 && table.getNullElements() == 2;
        List<Integer> list=table.removeMany(false,0);
        assert list.size() == 12;
        assertIndices(table, 0, 12, 20);
        assert table.size() == 6 && table.getNullElements() == 2;
        table.purge(12);
        assertIndices(table, 12, 12, 20);
        assert table.size() == 6 && table.getNullElements() == 2;
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
            if(count < 10) {
                assert row == 0;
                assert column == count;
            }
            else {
                assert row == 1;
                assert column == count - 10;
            }
            count++;
        }
    }

    public static void testGetNullMessages() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        table.add(1, 1);
        table.add(100, 100);
        System.out.println("table = " + table);
        int num_null_elements=table.getNullElements();
        assert num_null_elements == 98; // [2 .. 99]

        table.add(50,50);
        System.out.println("table = " + table);
        assert table.size() == 3;
        assert table.getNullElements() == 97;
    }

    public static void testGetNullMessages2() {
        Table<Integer> table=new Table<Integer>(1, 10, 0);
        table.add(1, 1);
        table.add(5, 5);
        System.out.println("table = " + table);
        int num_null_elements=table.getNullElements();
        assert num_null_elements == 3; // [2 .. 4]

        table.add(10,10);
        System.out.println("table = " + table);
        assert table.size() == 3;
        assert table.getNullElements() == 7;

        table.add(14,14);
        System.out.println("table = " + table);
        assert table.size() == 4;
        assert table.getNullElements() == 10;

        while(table.remove() != null)
            ;
        System.out.println("table = " + table);
        assert table.size() == 3;
        assert table.getNullElements() == 10;
    }



    public static void testMassAddition() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        final int NUM_ELEMENTS=10005;
        for(int i=1; i <= NUM_ELEMENTS; i++)
            table.add(i, i);
        System.out.println("table = " + table);
        assert table.size() == NUM_ELEMENTS;
        assert table.capacity() == 10010;
        assertIndices(table, 0, 0, NUM_ELEMENTS);
        assert table.getNullElements() == 0;
    }

    public static void testResize() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        assert table.capacity() == 30;
        addAndGet(table, 30);
        addAndGet(table, 35);
        assert table.capacity() == 40;
        addAndGet(table, 500);
        assert table.capacity() == 510;

        addAndGet(table, 515);
        assert table.capacity() == 520;
    }

    public void testResizeWithPurge() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 100; i++)
            addAndGet(table, i);
        System.out.println("table: " + table);
        
        // now remove 60 messages
        for(int i=1; i <= 60; i++) {
            Integer num=table.remove();
            assert num != null && num.intValue() == i;
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
        assert table.size() == 50 && table.getNullElements() == 0;

        // now remove 15 messages
        for(long i=1; i <= 15; i++) {
            Integer num=table.remove(false);
            assert num != null && num.intValue() == i;
        }
        System.out.println("table after removal of seqno 15: " + table);
        assertIndices(table, 0, 15, 50);
        assert table.size() == 35 && table.getNullElements() == 0;

        table.purge(15);
        System.out.println("now triggering a resize() by addition of seqno=55");
        addAndGet(table, 55);
        assertIndices(table, 15, 15, 55);
        assert table.size() == 36 && table.getNullElements() == 4;

        // now we have elements 40-49 in row 1 and 55 in row 2:
        List<Integer> list=new ArrayList<Integer>(20);
        for(int i=16; i < 50; i++)
            list.add(i);
        list.add(55);

        for(long i=table.getOffset(); i < table.capacity() + table.getOffset(); i++) {
            Integer num=table.get(i);
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
        assert table.capacity() == 60;
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
            assert num != null && num.intValue() == i;
        }

        assert table.get(50) != null;
        assert table.get(51) == null;
        Integer num=table.get(52);
        assert num != null && num.intValue() == 52;
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
        assert table.capacity() == 50;
    }


    public static void testPurge() {
        Table<Integer> table=new Table<Integer>(5, 10, 0);
        for(int seqno=1; seqno <= 25; seqno++)
            table.add(seqno, seqno);

        int[] seqnos={30,31,32,37,38,39, 40,41,42,47,48,49};
        for(int seqno: seqnos)
            table.add(seqno, seqno);

        System.out.println("table (before remove):\n" + table.dump());
        for(int seqno=0; seqno <= 22; seqno++)
            table.remove(false);

        System.out.println("\ntable (after remove 22, before purge):\n" + table.dump());
        table.purge(22);
        System.out.println("\ntable: (after purge 22):\n" + table.dump());
        assert table.size() == 3 + seqnos.length;
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
        assert table.capacity() == 40;
    }


    public void testCompactWithAutomaticPurging() {
        Table<Integer> table=new Table<Integer>(3, 10, 0);
        for(int i=1; i <= 80; i++)
             addAndGet(table, i);
        assert table.size() == 80;
        for(long i=1; i <= 60; i++)
            table.remove();

        assert table.size() == 20;
        table.compact();
        assert table.size() == 20;
        assert table.capacity() == 40;
    }




    protected static void addAndGet(Table<Integer> table, int seqno) {
        boolean added=table.add((long)seqno,seqno);
        assert added;
        Integer val=table.get(seqno);
        assert val != null && val == seqno;
    }

    protected static <T> void assertIndices(Table<T> table, long low, long hd, long hr) {
        assert table.getLow() == low : "expected low=" + low + " but was " + table.getLow();
        assert table.getHighestDelivered() == hd : "expected hd=" + hd + " but was " + table.getHighestDelivered();
        assert table.getHighestReceived()  == hr : "expected hr=" + hr + " but was " + table.getHighestReceived();
    }

}
