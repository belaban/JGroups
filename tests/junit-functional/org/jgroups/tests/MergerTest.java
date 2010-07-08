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
 * @version $Id: MergerTest.java,v 1.3 2010/07/08 11:51:49 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class MergerTest {
    private final static Address a=Util.createRandomAddress("A"),
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
         Map<Address,View> map=new HashMap<Address,View>();
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


    /**
     * A: ABC
     * B: BC
     * C: BC
     */
    public void testOverlappingMerge() {
        Map<Address,View> map=new HashMap<Address,View>();
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
     * A: AB
     * B: B
     */
    public void testOverlappingMerge2() {
        Map<Address,View> map=new HashMap<Address,View>();
        map.put(a, makeView(a, a,b));
        map.put(b, makeView(b, b));
        System.out.println("map:\n" + print(map));

        assert map.size() == 2;
        Merger.sanitizeViews(map);
        System.out.println("map after sanitization:\n" + print(map));

        assert map.size() == 2;
        assert map.get(a).size() == 1;
        assert map.get(b).size() == 1;
    }


     /**
      * A: AB
      * B: BC
      * C: CD
      * D: DE
      */
     public void testOverlappingMerge3() {
         Map<Address,View> map=new HashMap<Address,View>();
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


    private static <T> Collection<T> makeList(T ... elements) {
        return new ArrayList<T>(Arrays.asList(elements));
    }

    private static View makeView(Address coord, Address ... members) {
        return new View(coord, 1, new Vector<Address>(Arrays.asList(members)));
    }

    static String print(Map<Address,View> map) {
        StringBuilder sb=new StringBuilder();
        for(Map.Entry<Address,View> entry: map.entrySet()) {
            sb.append(entry.getKey()).append(": ").append(entry.getValue().getMembers()).append("\n");
        }
        return sb.toString();
    }


}
