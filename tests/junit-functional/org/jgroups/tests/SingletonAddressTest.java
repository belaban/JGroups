package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.SingletonAddress;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link SingletonAddress}
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class SingletonAddressTest {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");

    public static void testCompareTo() {
        SingletonAddress sa=new SingletonAddress("cluster".getBytes(), a);
        SingletonAddress sb=new SingletonAddress("cluster".getBytes(), a);
        assert sa.equals(sb);
        assert sa.compareTo(sb) == 0;

        sb=new SingletonAddress("cluster".getBytes(), b);
        assert !sa.equals(sb);
        assert sa.compareTo(sb) != 0;

        sb=new SingletonAddress("cluster2".getBytes(), a);
        assert !sa.equals(sb);
        assert sa.compareTo(sb) != 0;
    }

    public static void testCompareTo2() {
        SingletonAddress sa=new SingletonAddress("cluster".getBytes(), a);
        SingletonAddress sb=new SingletonAddress("cluster".getBytes(), b);
        SingletonAddress sc=new SingletonAddress("cluster".getBytes(), a);
        Map<Address,Integer> map=new HashMap<>(3);
        map.put(sa, 1);
        map.put(sb,2);
        map.put(sc,3);
        System.out.println("map = " + map);
        assert map.size() == 2;
        assert map.keySet().contains(sa);
        assert map.keySet().contains(sc);
    }
}
