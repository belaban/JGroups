package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.Range;
import org.jgroups.util.Util;
import org.jgroups.util.XmitRange;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @author Bela Ban
 * @version $Id: XmitRangeTest.java,v 1.6 2009/11/25 08:55:07 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=false)
public class XmitRangeTest {

    public static void testConstructor() {
        XmitRange range=new XmitRange(10, 10);
        System.out.println(print(range));
        assert range.size() == 1;
        assert range.getLow() == 10;
        assert range.getHigh() == 10;
        assert range.contains(10);
        assert !range.contains(11);

        range=new XmitRange(10, 15);
        System.out.println(print(range));
        assert range.size() == 6;
        assert range.getLow() == 10;
        assert range.getHigh() == 15;
        assert range.contains(10);
        assert range.contains(14);
    }

    public static void testSetAndGetWith1Seqno() {
        XmitRange range=new XmitRange(10, 10);
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
        XmitRange range=new XmitRange(10, 15);
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
        XmitRange range=new XmitRange(10, 15);
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


        range=new XmitRange(10, 15);
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

        range=new XmitRange(10, 15);
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

        range=new XmitRange(10, 15);
        range.set(10,11,12,13,14,15);
        System.out.println("range=" + print(range));
        assert range.size() == 6;
        assert range.getNumberOfReceivedMessages() == 6;
        assert range.getNumberOfMissingMessages() == 0;
        xmits=range.getMessagesToRetransmit();
        assert xmits.isEmpty();

        range=new XmitRange(10, 15);
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
        XmitRange range=new XmitRange(10, 10);
        range.set(9);
    }
    
    
    public static void testCompareTo() {
        TreeMap<XmitRange,XmitRange> map=new TreeMap<XmitRange,XmitRange>();

        XmitRange[] ranges=new XmitRange[]{new XmitRange(900,905), new XmitRange(222,222), new XmitRange(700,800), new XmitRange(23,200)};

        for(XmitRange range: ranges)
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
        XmitRange range=new XmitRange(0, 1500);

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


    public static void testRemovalFromHashMap() {
        TreeMap<XmitRange,XmitRange> map=new TreeMap<XmitRange,XmitRange>();

        XmitRange[] ranges=new XmitRange[]{new XmitRange(900,905), new XmitRange(222,222), new XmitRange(700,800), new XmitRange(23,200)};

        for(XmitRange range: ranges)
            map.put(range, range);

        System.out.println("map = " + map.keySet());
        assert map.size() == 4;

        XmitRange r=map.get(new XmitRange(222, true));
        assert r != null;
        map.remove(r);
        assert map.size() == 3;

        r=map.get(new XmitRange(108, true));
        assert r != null;
        map.remove(r);
        assert map.size() == 2;

        r=map.get(new XmitRange(902, true));
        assert r != null;
        map.remove(r);
        assert map.size() == 1;

        r=map.get(new XmitRange(800, true));
        assert r != null;
        map.remove(r);
        assert map.isEmpty();
    }


    private static void checkInRange(Map<XmitRange,XmitRange> map, long seqno, long from, long to) {
        XmitRange val=map.get(new XmitRange(seqno, true));
        System.out.println("seqno=" + seqno + ", val = " + val);
        assert val.contains(seqno);
        assert val.getLow() == from;
        assert val.getHigh() == to;
    }

    private static void checkNull(Map<XmitRange,XmitRange> map, long seqno) {
        XmitRange val=map.get(new XmitRange(seqno, true));
        assert val == null;
    }


    private static String print(XmitRange range) {
        StringBuilder sb=new StringBuilder();
        sb.append("low=" + range.getLow() + ", high=" + range.getHigh());
        sb.append( ", size= " + range.size());
        sb.append(", received=" + range.printBits(true) + " (" + range.getNumberOfReceivedMessages() + ")");
        sb.append(", missing=" + range.printBits(false) + " (" + range.getNumberOfMissingMessages() + ")");
        return sb.toString();
    }
}
