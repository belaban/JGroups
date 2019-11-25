package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.*;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class SeqnoTest {

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
        Seqno range=new SeqnoRange(10, 10);
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

    public static void testGetBits() {
        SeqnoRange range=new SeqnoRange(1, 100);
        System.out.println("range = " + range);
        assert range.size() == 100;

        Collection<Range> bits=range.getBits(false);
        assert bits.size() == 1;
        Range tmp=bits.iterator().next();
        assert tmp.low == 1 && tmp.high == 100;

        range.set(1,2);
        assert range.size() == 100;

        bits=range.getBits(true);
        assert bits != null && bits.size() == 1; // 1 range: [1-2]
        tmp=bits.iterator().next();
        assert tmp.low == 1 && tmp.high == 2;

        for(int i=1; i < 100; i++)
            range.set(i);

        bits=range.getBits(false);
        assert bits.size() == 1;
        tmp=bits.iterator().next();
        assert tmp.low == 100 && tmp.high == 100;

        for(int i=1; i <= range.size(); i++)
            range.clear(i);

        for(int i=2; i <= 99; i++)
            range.set(i);

        bits=range.getBits(true);
        assert bits.size() == 1;
        tmp=bits.iterator().next();
        assert tmp.low == 2 && tmp.high == 99;

        bits=range.getBits(false);
        assert bits.size() == 2;

        tmp=bits.iterator().next();
        assert tmp.low == 1 && tmp.high == 1;

        Iterator<Range> it=bits.iterator();
        it.next();
        tmp=it.next();
        assert tmp.low == 100 && tmp.high == 100;
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
        TreeMap<Seqno,Seqno> map=new TreeMap<>(new SeqnoComparator());

        Seqno[] ranges={new SeqnoRange(900,905), new Seqno(222), new SeqnoRange(700,800), new SeqnoRange(23,200)};

        for(Seqno range: ranges)
            map.put(range, range);

        System.out.println("map = " + map.keySet());
        assert map.size() == ranges.length;

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


    public static void testCompareTo2() {
        TreeMap<Seqno,Seqno> map=new TreeMap<>(new SeqnoComparator());

        Seqno[] ranges={new SeqnoRange(900,905), new Seqno(550), new Seqno(222),
          new SeqnoRange(700,800), new Seqno(650), new SeqnoRange(23,200)};

        for(Seqno range: ranges)
            map.put(range, range);

        System.out.println("map = " + map.keySet());
        assert map.size() == 6;

        for(long num: new long[]{0, 1, 201, 202, 223, 1000}) {
            checkNull(map, num);
        }

        checkInRange(map, 550, 550, 550);
        checkInRange(map, 650, 650, 650);

        checkInRange(map,  23,  23, 200);
        checkInRange(map, 100,  23, 200);
        checkInRange(map, 200,  23, 200);
        checkInRange(map, 222, 222, 222);
        checkInRange(map, 750, 700, 800);
        checkInRange(map, 905, 900, 905);
    }



    public static void testLargeRange() {
        SeqnoRange range=new SeqnoRange(0, 1500);

        Set<Integer> sorted_set=new TreeSet<>();
        for(int i=0; i < 500; i++) {
            int num=(int)Util.random(1499);
            sorted_set.add(num);
        }

        sorted_set.forEach(range::set);

        int num_set=sorted_set.size();
        System.out.println("set " + num_set + " bits");
        assert range.getNumberOfReceivedMessages() == num_set;
        Collection<Range> missing=range.getMessagesToRetransmit();
        System.out.println("missing = " + missing);
    }


    public static void testRemovalFromTreeMap() {
        Map<Seqno,Seqno> map=new TreeMap<>(new SeqnoComparator());

        Seqno[] ranges={new SeqnoRange(900, 905), new Seqno(222), new Seqno(500),
          new SeqnoRange(700,800), new Seqno(801), new SeqnoRange(23,200)};

        for(Seqno range: ranges)
            map.put(range, range);

        System.out.println("map = " + map.keySet());
        assert map.size() == ranges.length;

        for(Seqno r: ranges) {
            Seqno range=map.get(r);
            assert range != null;
            assert range == r; // must point to the same object in memory
        }

        for(Seqno r: ranges) {
            Seqno range=map.remove(r);
            assert range != null;
            assert range == r;
        }

        assert map.isEmpty();
    }


    public static void testRemovalFromHashMap() {
        Map<Seqno,Seqno> map=new ConcurrentHashMap<>();

        Seqno[] ranges={new SeqnoRange(900, 905), new Seqno(222), new SeqnoRange(700, 800),
          new SeqnoRange(23,200), new Seqno(201), new Seqno(205)};

        for(Seqno range: ranges)
            map.put(range, range);

        System.out.println("map = " + map.keySet());
        assert map.size() == ranges.length;

        for(Seqno r: ranges) {
            Seqno range=map.get(r);
            assert range != null;
            assert range == r; // must point to the same object in memory
        }

        for(Seqno r: ranges) {
            Seqno range=map.remove(r);
            assert range != null;
            assert range == r;
        }

        assert map.isEmpty();
    }


    private static void checkInRange(Map<Seqno,Seqno> map, long seqno, long from, long to) {
        Seqno val=map.get(new Seqno(seqno, true));
        System.out.println("seqno=" + seqno + ", val = " + val);
        assert val != null;
        assert val.contains(seqno);
        assert val.getLow() == from;
        if(val instanceof SeqnoRange)
            assert ((SeqnoRange)val).getHigh() == to;
    }

    private static void checkNull(Map<Seqno,Seqno> map, long seqno) {
        Seqno val=map.get(new Seqno(seqno, true));
        assert val == null;
    }


    private static String print(Seqno seqno) {
        StringBuilder sb=new StringBuilder();
        sb.append(seqno.toString());
        sb.append(", size= " + seqno.size());
        if(seqno instanceof SeqnoRange) {
            sb.append(", received=" + ((SeqnoRange)seqno).printBits(true) + " (" + seqno.getNumberOfReceivedMessages() + ")");
            sb.append(", missing=" + ((SeqnoRange)seqno).printBits(false) + " (" + seqno.getNumberOfMissingMessages() + ")");
        }
        return sb.toString();
    }
}
