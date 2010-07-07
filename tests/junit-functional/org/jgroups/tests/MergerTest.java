package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.pbcast.Merger;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @author Bela Ban
 * @version $Id: MergerTest.java,v 1.1 2010/07/07 09:38:51 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class MergerTest {
    private final Address a=Util.createRandomAddress("A"),
            b=Util.createRandomAddress("B"),
            c=Util.createRandomAddress("C"),
            d=Util.createRandomAddress("D"),
            e=Util.createRandomAddress("E"),
            f=Util.createRandomAddress("F");


     /**
      * A: AB
      * B: AB
      * C: CD
      * D: CD
     */
     public void testSimpleMerge() {
         Map<Address, Collection<Address>> map=new HashMap<Address,Collection<Address>>();
         map.put(a, makeList(a,b));
         map.put(b, makeList(a,b));
         map.put(c, makeList(c,d));
         map.put(d, makeList(c,d));
         System.out.println("map = " + map);

         assert map.size() == 4;
         Merger.sanitize(map);
         System.out.println("map after sanitization: " + map);

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
        Map<Address, Collection<Address>> map=new HashMap<Address,Collection<Address>>();
        map.put(a, makeList(a,b,c));
        map.put(b, makeList(b,c));
        map.put(c, makeList(b,c));
        System.out.println("map = " + map);

        assert map.size() == 3;
        Merger.sanitize(map);
        System.out.println("map after sanitization: " + map);

        assert map.size() == 3;
        assert map.get(a).size() == 1;
        assert map.get(b).size() == 2;
        assert map.get(c).size() == 2;
    }


    /**
     * A: AB
     * B: B
     */
    public void testOverlappingMerge2() {
        Map<Address, Collection<Address>> map=new HashMap<Address,Collection<Address>>();
        map.put(a, makeList(a,b));
        map.put(b, makeList(b));
        System.out.println("map = " + map);

        assert map.size() == 2;
        Merger.sanitize(map);
        System.out.println("map after sanitization: " + map);

        assert map.size() == 2;
        assert map.get(a).size() == 1;
        assert map.get(b).size() == 1;
    }

    private static <T> Collection<T> makeList(T ... elements) {
        return new ArrayList<T>(Arrays.asList(elements));
    }

}
