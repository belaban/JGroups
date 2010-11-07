package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.FixedSizeBitSet;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.TreeSet;

/**
 * @author Bela Ban
 * @version $Id: FixedSizeBitSetTest.java,v 1.2 2009/11/25 08:48:55 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
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
        set.set(10);
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
        FixedSizeBitSet set=new FixedSizeBitSet(10);
        System.out.println("set = " + set);
        set.set(0);
        set.set(9);
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
        Set<Integer> sorted_set=new TreeSet<Integer>();
        for(int i=0; i < 500; i++) {
            int num=(int)Util.random(1499);
            sorted_set.add(num);
        }

        for(int num: sorted_set)
            set.set(num);

        int num_set=sorted_set.size();
        System.out.println("set " + num_set + " bits");
        assert set.cardinality() == num_set;
    }

}




