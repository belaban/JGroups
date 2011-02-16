package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.ProxyAddress;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class LazyRemovalSetTest {

    public static void testAdd() {
        LazyRemovalSet<UUID> cache=new LazyRemovalSet<UUID>();
        UUID uuid=UUID.randomUUID();
        cache.add(uuid);
        System.out.println("cache = " + cache);
        assert 1 == cache.size();
        assert cache.contains(uuid);

        cache.remove(uuid);
        System.out.println("cache = " + cache);

        assert cache.contains(uuid);
    }

    public static void testRemoveAll() {
        LazyRemovalSet<String> cache=new LazyRemovalSet<String>(10, 0);
        cache.add("one", "two", "three", "four", "five", "two");
        System.out.println("cache = " + cache);
        assert cache.size() == 5;

        List<String> list=Arrays.asList("four", "two");
        System.out.println("removing " + list);
        cache.removeAll(list);
        System.out.println("cache = " + cache);
        assert cache.size() == 5;

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 3;
        assert cache.contains("one");
        assert cache.contains("three");
        assert cache.contains("five");
    }

    public static void testRetainAll() {
        LazyRemovalSet<String> cache=new LazyRemovalSet<String>(10, 0);
        cache.add("one", "two", "three");
        System.out.println("cache = " + cache);
        assert cache.size() == 3;

        List<String> retain=Arrays.asList("two", "three");
        System.out.println("retaining " + retain);
        cache.retainAll(retain);
        System.out.println("cache = " + cache);
        assert cache.size() == 3;
        assert cache.contains("two");
        assert cache.contains("three");

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 2;
    }


    public static void testRemovalOnExceedingMaxSize() {
        LazyRemovalSet<String> cache=new LazyRemovalSet<String>(2, 0);
        cache.add("u1", "u2", "u3", "u4");
        assert cache.size() == 4;

        cache.remove("u3");
        System.out.println("cache = " + cache);
        assert cache.size() == 3;

        cache.remove("u1");
        System.out.println("cache = " + cache);
        assert cache.size() == 2;

        cache.remove("u4");
        System.out.println("cache = " + cache);
        assert cache.size() == 2;

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 1;
    }


    public static void testRemovalOnExceedingMaxSizeAndMaxTime() {
        LazyRemovalSet<String> cache=new LazyRemovalSet<String>(2, 1000);
        cache.add("u1", "u2", "u3", "u4");
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove("u3");
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove("u1");
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove("u4");
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        System.out.println("sleeping for 1 sec");
        Util.sleep(1100);
        cache.remove("u4");
        System.out.println("cache = " + cache);
        assert cache.size() == 1;
    }


    public static void testCapacityExceeded() {
        LazyRemovalSet<Integer> cache=new LazyRemovalSet<Integer>(5, 0);
        for(int i=1; i <=10; i++)
            cache.add(i);
        System.out.println("cache = " + cache);
        assert cache.size() == 10;
        cache.retainAll(Arrays.asList(1,4,6,8));
        cache.add(11);
        System.out.println("cache = " + cache);
        assert cache.size() == 5;
    }


    public static void testContains() {
        LazyRemovalSet<Address> cache=new LazyRemovalSet<Address>(5, 0);
        Address a=Util.createRandomAddress("A"),
          b=new ProxyAddress(Util.createRandomAddress("X"), Util.createRandomAddress("B")),
          c=Util.createRandomAddress("C"),
          d=new ProxyAddress(Util.createRandomAddress("Y"), Util.createRandomAddress("D"));
        cache.add(a,b,c,d);
        System.out.println("cache = " + cache);
        assert cache.size() == 4;
        assert cache.contains(a);
        assert cache.contains(b);
        assert cache.contains(d);
        assert cache.contains(d);
        cache.retainAll(Arrays.asList(a,c));
        System.out.println("cache = " + cache);
        assert cache.size() == 4;
        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 2;
    }


}
