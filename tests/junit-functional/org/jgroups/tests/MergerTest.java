package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.Merger;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class MergerTest {
    private final static Address a=Util.createRandomAddress("A"),
      b=Util.createRandomAddress("B"),
      c=Util.createRandomAddress("C"),
      d=Util.createRandomAddress("D"),
      e=Util.createRandomAddress("E"),
      f=Util.createRandomAddress("F"),
      x=Util.createRandomAddress("X"),
      y=Util.createRandomAddress("Y"),
      z=Util.createRandomAddress("Z");
    

    /**
     * A: AB
     * B: AB
     * C: CD
     * D: CD
     */
    public void testSimpleMerge() {
        Map<Address,View> map=new HashMap<>();
        map.put(a, makeView(a, a,b));
        map.put(b, makeView(a, a,b));
        map.put(c, makeView(c, c,d));
        map.put(d, makeView(c, c,d));
        System.out.println("map:\n" + print(map));

        assert map.size() == 4;
        Merger.sanitizeViews(map);
        System.out.println("map after sanitization:\n" + print(map));

        assert map.size() == 4;
        assert map.get(a).size() == 2;
        assert map.get(b).size() == 2;
        assert map.get(c).size() == 2;
        assert map.get(d).size() == 2;
    }

    public void testSimpleMerge2() {
        Map<Address,View> map=new HashMap<>();
        map.put(a, makeView(a, a,b));
        map.put(b, makeView(a, a,b));
        map.put(c, makeView(c, c,d));
        map.put(d,makeView(c,c,d));
        System.out.println("map:\n" + print(map));

        assert map.size() == 4;
        Merger.sanitizeViews(map);
        System.out.println("map after sanitization:\n" + print(map));

        assert map.size() == 4;
        assert map.get(a).size() == 2;
        assert map.get(b).size() == 2;
        assert map.get(c).size() == 2;
        assert map.get(d).size() == 2;

    }


    /**
     * A: ABC
     * B: BC
     * C: BC
     */
    public void testOverlappingMerge() {
        Map<Address,View> map=new HashMap<>();
        map.put(a, makeView(a, a,b,c));
        map.put(b, makeView(b, b,c));
        map.put(c, makeView(b, b,c));
        System.out.println("map:\n" + print(map));

        assert map.size() == 3;
        Merger.sanitizeViews(map);
        System.out.println("map after sanitization:\n" + print(map));

        assert map.size() == 3;
        assert map.get(a).size() == 1;
        assert map.get(b).size() == 2;
        assert map.get(c).size() == 2;
    }

    /**
     * A: BAC
     * B: BC
     * C: BC
     */
    public void testOverlappingMerge1() {
        Map<Address,View> map=new HashMap<>();
        map.put(a, makeView(b, b,a,c));
        map.put(b, makeView(b, b,c));
        map.put(c, makeView(b, b,c));
        System.out.println("map:\n" + print(map));

        Collection<Address> merge_coords=Util.determineMergeCoords(map);
        System.out.println("merge_coords = " + merge_coords);
        assert merge_coords.size() == 1;

        Collection<Address> merge_participants=Util.determineMergeParticipants(map);
        System.out.println("merge_participants = " + merge_participants);
        assert merge_participants.size() == 2;
    }


    /**
     * A: ABCDEF
     * X: XYZ
     */
    public void testOverlappingMerge2() {
        Map<Address,View> map=new HashMap<>();
        map.put(a, makeView(a, a,b,c,d,e,f));
        map.put(x, makeView(x, x,y,z));
        System.out.println("map:\n" + print(map));

        assert map.size() == 2;
        Collection<Address> merge_coords=Util.determineMergeCoords(map);
        System.out.println("merge_coords = " + merge_coords);
        assert merge_coords.size() == 2;

        Collection<Address> merge_participants=Util.determineMergeParticipants(map);
        System.out.println("merge_participants = " + merge_participants);
        assert merge_participants.size() == 2;
    }


    /**
     * A: AB
     * B: BC
     * C: CD
     * D: DE
     */
    public void testOverlappingMerge3() {
        Map<Address,View> map=new HashMap<>();
        map.put(a, makeView(a, a,b));
        map.put(b, makeView(b, b,c));
        map.put(c, makeView(c, c,d));
        map.put(d, makeView(d, d,e));
        System.out.println("map:\n" + print(map));

        assert map.size() == 4;
        Merger.sanitizeViews(map);
        System.out.println("map after sanitization:\n" + print(map));

        assert map.size() == 4;
        assert map.get(a).size() == 1;
        assert map.get(b).size() == 1;
        assert map.get(c).size() == 1;
        assert map.get(d).size() == 2;
    }

    /**
     * A: ACB
     * B: ACB
     * C: ACB
     * D: BACD
     * Test case is https://issues.jboss.org/browse/JGRP-1451
     */
    public void testOverlappingMerge4() {
        Map<Address,View> map=new HashMap<>();
        map.put(a, makeView(a, a,c,b));
        map.put(b, makeView(a, a,c,b));
        map.put(c, makeView(a, a,c,b));
        map.put(d, makeView(d, b,a,c,d));
        System.out.println("map:\n" + print(map));

        assert map.size() == 4;
        Merger.sanitizeViews(map);
        System.out.println("map after sanitization:\n" + print(map));

        assert map.size() == 4;
        assert map.get(a).size() == 3;
        assert map.get(b).size() == 3;
        assert map.get(c).size() == 3;
        assert map.get(d).size() == 1;
    }


    private static <T> Collection<T> makeList(T ... elements) {
        return new ArrayList<>(Arrays.asList(elements));
    }

    private static View makeView(Address coord, Address ... members) {
        return View.create(coord, 1, members);
    }

    static String print(Map<Address,View> map) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,View> entry: map.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue().getMembers()).append("\n");
        }
        return sb.toString();
    }


}
