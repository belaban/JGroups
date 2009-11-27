package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Range;
import org.jgroups.util.Util;
import org.jgroups.util.SeqnoRange;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Bela Ban
 * @version $Id: SeqnoRangeTest.java,v 1.1 2009/11/27 15:49:38 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class SeqnoRangeTest {

    public static void testConstructor() {
        SeqnoRange range=new SeqnoRange(10, 10);
        System.out.println(print(range));
        assert range.size() == 1;
        assert range.getLow() == 10;
        assert range.getHigh() == 10;
        assert range.contains(10);
        assert !range.contains(11);

        range=new SeqnoRange(10, 15);
        System.out.println(print(range));
        assert range.size() == 6;
        assert range.getLow() == 10;
        assert range.getHigh() == 15;
        assert range.contains(10);
        assert range.contains(14);
    }

    public static void testSetAndGetWith1Seqno() {
        SeqnoRange range=new SeqnoRange(10, 10);
        assert range.getNumberOfMissingMessages() == 1;
        assert range.getNumberOfReceivedMessages() == 0;

        range.set(10);
        assert range.getNumberOfMissingMessages() == 0;
        assert range.getNumberOfReceivedMessages() == 1;

        assert range.get(10);

        range.clear(10);
        assert !range.get(10);
        assert range.getNumberOfMissingMessages() == 1;
        assert range.getNumberOfReceivedMessages() == 0;
    }

    public static void testSetAndGetWith5Seqnos() {
        SeqnoRange range=new SeqnoRange(10, 15);
        System.out.println("range=" + print(range));

        assert range.size() == 6;
        assert range.getNumberOfMissingMessages() == 6;
        assert range.getNumberOfReceivedMessages() == 0;

        range.set(10);
        assert range.getNumberOfMissingMessages() == 5;
        assert range.getNumberOfReceivedMessages() == 1;

        assert range.get(10);

        range.set(13);
        assert range.size() == 6;
        assert range.getNumberOfMissingMessages() == 4;
        assert range.getNumberOfReceivedMessages() == 2;

        range.set(13);
        assert range.size() == 6;
        assert range.getNumberOfMissingMessages() == 4;
        assert range.getNumberOfReceivedMessages() == 2;

        System.out.println("range=" + print(range));

        Collection<Range> xmits=range.getMessagesToRetransmit();
        Collection<Range> cleared_bits=range.getBits(false);

        System.out.println("xmits = " + xmits);
        System.out.println("cleared_bits = " + cleared_bits);

        assert xmits.equals(cleared_bits);
    }


    public static void testSet() {
        SeqnoRange range=new SeqnoRange(10, 15);
        range.set(11, 12, 13, 14);
        System.out.println("range=" + print(range));
        assert range.size() == 6;
        assert range.getNumberOfReceivedMessages() == 4;
        assert range.getNumberOfMissingMessages() == 2;
        Collection<Range> xmits=range.getMessagesToRetransmit();
        assert xmits.size() == 2;
        Iterator<Range> it=xmits.iterator();
        Range r=it.next();
        assert r.low == 10 && r.high == 10;
        r=it.next();
        assert r.low == 15 && r.high == 15;


        range=new SeqnoRange(10, 15);
        range.set(10,11,12,13,14);
        System.out.println("range=" + print(range));
        assert range.size() == 6;
        assert range.getNumberOfReceivedMessages() == 5;
        assert range.getNumberOfMissingMessages() == 1;
        xmits=range.getMessagesToRetransmit();
        assert xmits.size() == 1;
        it=xmits.iterator();
        r=it.next();
        assert r.low == 15 && r.high == 15;

        range=new SeqnoRange(10, 15);
        range.set(11,12,13,14,15);
        System.out.println("range=" + print(range));
        assert range.size() == 6;
        assert range.getNumberOfReceivedMessages() == 5;
        assert range.getNumberOfMissingMessages() == 1;
        xmits=range.getMessagesToRetransmit();
        assert xmits.size() == 1;
        it=xmits.iterator();
        r=it.next();
        assert r.low == 10 && r.high == 10;

        range=new SeqnoRange(10, 15);
        range.set(10,11,12,13,14,15);
        System.out.println("range=" + print(range));
        assert range.size() == 6;
        assert range.getNumberOfReceivedMessages() == 6;
        assert range.getNumberOfMissingMessages() == 0;
        xmits=range.getMessagesToRetransmit();
        assert xmits.isEmpty();

        range=new SeqnoRange(10, 15);
        range.set(11,12,14,15);
        System.out.println("range=" + print(range));
        assert range.size() == 6;
        assert range.getNumberOfReceivedMessages() == 4;
        assert range.getNumberOfMissingMessages() == 2;
        xmits=range.getMessagesToRetransmit();
        assert xmits.size() == 2;
        it=xmits.iterator();
        r=it.next();
        assert r.low == 10 && r.high == 10;
        r=it.next();
        assert r.low == 13 && r.high == 13;

        range.set(13);
        assert range.getNumberOfReceivedMessages() == 5;
        assert range.getNumberOfMissingMessages() == 1;
        xmits=range.getMessagesToRetransmit();
        it=xmits.iterator();
        r=it.next();
        assert r.low == 10 && r.high == 10;

        range.set(10);
        System.out.println("range=" + print(range));
        assert range.getNumberOfReceivedMessages() == 6;
        assert range.getNumberOfMissingMessages() == 0;
        xmits=range.getMessagesToRetransmit();
        assert xmits.isEmpty();
    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public static void testSetOfInvalidIndex() {
        SeqnoRange range=new SeqnoRange(10, 10);
        range.set(9);
    }
    
    
    public static void testCompareTo() {
        TreeMap<SeqnoRange, SeqnoRange> map=new TreeMap<SeqnoRange, SeqnoRange>();

        SeqnoRange[] ranges=new SeqnoRange[]{new SeqnoRange(900,905), new SeqnoRange(222,222), new SeqnoRange(700,800), new SeqnoRange(23,200)};

        for(SeqnoRange range: ranges)
            map.put(range, range);

        System.out.println("map = " + map.keySet());
        assert map.size() == 4;

        for(long num: new long[]{0, 1, 201, 202, 223, 1000}) {
            checkNull(map, num);
        }

        checkInRange(map,  23,  23, 200);
        checkInRange(map, 100,  23, 200);
        checkInRange(map, 200,  23, 200);
        checkInRange(map, 222, 222, 222);
        checkInRange(map, 750, 700, 800);
        checkInRange(map, 905, 900, 905);
    }


    public static void testLargeRange() {
        SeqnoRange range=new SeqnoRange(0, 1500);

        Set<Integer> sorted_set=new TreeSet<Integer>();
        for(int i=0; i < 500; i++) {
            int num=(int)Util.random(1499);
            sorted_set.add(num);
        }

        for(int num: sorted_set)
            range.set(num);

        int num_set=sorted_set.size();
        System.out.println("set " + num_set + " bits");
        assert range.getNumberOfReceivedMessages() == num_set;
        Collection<Range> missing=range.getMessagesToRetransmit();
        System.out.println("missing = " + missing);
    }


    public static void testRemovalFromTreeMap() {
        Map<SeqnoRange, SeqnoRange> map=new TreeMap<SeqnoRange, SeqnoRange>();

        SeqnoRange[] ranges=new SeqnoRange[]{new SeqnoRange(900,905), new SeqnoRange(222,222), new SeqnoRange(700,800), new SeqnoRange(23,200)};

        for(SeqnoRange range: ranges)
            map.put(range, range);

        System.out.println("map = " + map.keySet());
        assert map.size() == 4;

        SeqnoRange r=map.get(new SeqnoRange(222, true));
        assert r != null;
        map.remove(r);
        assert map.size() == 3;

        r=map.get(new SeqnoRange(108, true));
        assert r != null;
        map.remove(r);
        assert map.size() == 2;

        r=map.get(new SeqnoRange(902, true));
        assert r != null;
        map.remove(r);
        assert map.size() == 1;

        r=map.get(new SeqnoRange(800, true));
        assert r != null;
        map.remove(r);
        assert map.isEmpty();
    }


     public static void testRemovalFromHashMap() {
        Map<SeqnoRange, SeqnoRange> map=new ConcurrentHashMap<SeqnoRange, SeqnoRange>();

        SeqnoRange[] ranges=new SeqnoRange[]{new SeqnoRange(900,905), new SeqnoRange(222,222), new SeqnoRange(700,800), new SeqnoRange(23,200)};

        for(SeqnoRange range: ranges)
            map.put(range, range);

        System.out.println("map = " + map.keySet());
        assert map.size() == 4;

         for(SeqnoRange r: ranges) {
             SeqnoRange range=map.get(r);
             assert range != null;
             assert range == r; // must point to the same object in memory
         }

         for(SeqnoRange r: ranges) {
             SeqnoRange range=map.remove(r);
             assert range != null;
             assert range == r;
         }

         assert map.isEmpty();
    }


    private static void checkInRange(Map<SeqnoRange, SeqnoRange> map, long seqno, long from, long to) {
        SeqnoRange val=map.get(new SeqnoRange(seqno, true));
        System.out.println("seqno=" + seqno + ", val = " + val);
        assert val.contains(seqno);
        assert val.getLow() == from;
        assert val.getHigh() == to;
    }

    private static void checkNull(Map<SeqnoRange, SeqnoRange> map, long seqno) {
        SeqnoRange val=map.get(new SeqnoRange(seqno, true));
        assert val == null;
    }


    private static String print(SeqnoRange range) {
        StringBuilder sb=new StringBuilder();
        sb.append("low=" + range.getLow() + ", high=" + range.getHigh());
        sb.append( ", size= " + range.size());
        sb.append(", received=" + range.printBits(true) + " (" + range.getNumberOfReceivedMessages() + ")");
        sb.append(", missing=" + range.printBits(false) + " (" + range.getNumberOfMissingMessages() + ")");
        return sb.toString();
    }
}
