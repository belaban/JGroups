package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.FixedSizeBitSet;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.TreeSet;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class FixedSizeBitSetTest {

    public static void testConstructor() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        assert set.cardinality() == 0;
        assert set.size() == 10;
    }

    @Test(expectedExceptions=IndexOutOfBoundsException.class)
    public static void testSetWithIndexOutOfBounds() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        set.set(0);
        assert set.get(0);
        set.set(10);
    }

    public void testSet() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        assert set.set(4) && set.cardinality() == 1;
        assert !set.set(4);
        set.clear(4);
        assert set.cardinality() == 0;
        assert set.set(4) && set.cardinality() == 1;
    }

    public void testSetMultiple() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        set.set(4, 7);
        assert set.cardinality() == 4;
        assert set.get(4) && set.get(7);
        set.set(8,9);
        assert set.cardinality() == 6;

        set=new FixedSizeBitSet(260);
        set.set(2,5);
        set.set(30,230);
        assert set.cardinality() == 205;
        assert set.get(30) && set.get(230);
        System.out.println("set = " + set);

        set=new FixedSizeBitSet(10);
        set.set(5,5);
        assert set.cardinality() == 1 && set.get(5);
    }

    public static void testClear() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        set.set(0, 9);
        assert set.cardinality() == 10;
        set.clear(0);
        assert set.cardinality() == 9;
        set.clear(9);
        assert set.cardinality() == 8;
        assert !set.get(9);
        set.clear(5); set.clear(6);
        System.out.println("set = " + set);
        assert set.cardinality() == 6;
    }

    public static void testClearMultiple() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        set.set(0, 9);
        assert set.cardinality() == 10;
        set.clear(0, 0);
        assert set.cardinality() == 9;
        set.clear(0,1);
        assert set.cardinality() == 8;
        set.clear(6,9);
        assert set.cardinality() == 4;
    }

    public static void testClearMultiple2() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        set.set(0, 9);
        set.clear(5,8);
        assert set.cardinality() == 6;
    }

    public static void testClearMultiple3() {
        FixedSizeBitSet set=new FixedSizeBitSet(1000);
        set.set(0, 999);
        assert set.cardinality() == 1000;
        set.clear(100, 900);
        assert set.cardinality() == 199;
    }


    public static void testClearMultiple4() {
        FixedSizeBitSet set=new FixedSizeBitSet(1000);
        set.set(0, 999);
        assert set.cardinality() == 1000;
        set.clear(300, 999);
        assert set.cardinality() == 300;
    }


    public static void testClearMultiple5() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        set.set(0, 0);
        assert set.cardinality() == 1;
        set.clear(0);
        assert set.cardinality() == 0;
        set.set(0, 0);
        assert set.cardinality() == 1;
        set.clear(0,0);
        assert set.cardinality() == 0;
    }


    @Test(expectedExceptions=IndexOutOfBoundsException.class)
    public static void testClearWithIndexOutOfBounds() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        set.clear(10);
    }

    @Test(expectedExceptions=IndexOutOfBoundsException.class)
    public static void testGetWithIndexOutOfBounds() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        set.get(10);
    }


    public static void testToString() {
        FixedSizeBitSet set=new FixedSizeBitSet(100);
        for(int i=0; i < 100; i+=2)
            set.set(i);
        System.out.println("set = " + set);

        set=new FixedSizeBitSet(100);
        for(int i=1; i < 10; i+=2)
            set.set(i);
        set.set(15,20);
        for(int i=30; i < 60; i+=2)
            set.set(i);
        set.set(65,70);
        set.set(73,75);
        System.out.println("set = " + set);
    }


    public static void testNextSetBit() {
        FixedSizeBitSet set=new FixedSizeBitSet(64);
        int index=set.nextSetBit(10);
        assert index == -1 : "expected -1 but got " + index;

        index=set.nextSetBit(63);
        assert index == -1 : "expected -1 but got " + index;

        set.set(62);
        index=set.nextSetBit(62);
        assert index == 62 : "expected 62 but got " + index;

        index=set.nextSetBit(63);
        assert index == -1 : "expected -1 but got " + index;

        set.set(63);
        index=set.nextSetBit(63);
        assert index == 63 : "expected 63 but got " + index;
    }

    public static void testNextSetBit2() {
        FixedSizeBitSet set=new FixedSizeBitSet(64);
        set.set(0);
        int index=set.nextSetBit(0);
        assert index == 0 : "expected 0 but got " + index;
    }

    public void testCardinality() {
        FixedSizeBitSet set=new FixedSizeBitSet(20);
        for(int i=0; i < 20; i++)
            set.set(i);
        System.out.println("set = " + set);
        assert set.cardinality() == 20;
    }

    public static void testNextClearBit() {
        FixedSizeBitSet set=new FixedSizeBitSet(64);
        int index=set.nextClearBit(0);
        assert index == 0 : "expected 0 but got " + index;

        set.set(62); set.set(63);
        index=set.nextClearBit(62);
        assert index == -1;
    }



    public static void testNextSetAndClearBitOutOfRangeIndex() {
        FixedSizeBitSet set=new FixedSizeBitSet(64);
        for(int num: new int[]{64, 120}) {
            int index=set.nextSetBit(num);
            assert index == -1;
            index=set.nextClearBit(num);
            assert index == -1;
        }
    }


    public static void testLargeSet() {
        FixedSizeBitSet set=new FixedSizeBitSet(1500);
        Set<Integer> sorted_set=new TreeSet<>();
        for(int i=0; i < 500; i++) {
            int num=(int)Util.random(1499);
            sorted_set.add(num);
            set.set(num);
        }

        int num_set=sorted_set.size();
        System.out.println("set " + num_set + " bits");
        assert set.cardinality() == num_set;
    }

    public static void testFlip() {
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        for(int i=0; i < set.size(); i++)
            if(i % 2 == 0)
                set.set(i);
        System.out.println("set = " + set);
        set.flip();
        System.out.println("set = " + set);
        for(int i=0; i < set.size(); i++)
            assert set.get(i) == (i %2 != 0);
    }

}




