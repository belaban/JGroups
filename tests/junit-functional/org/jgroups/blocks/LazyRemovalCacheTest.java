package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class LazyRemovalCacheTest {

    public static void testAdd() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<>();
        UUID uuid=UUID.randomUUID();
        cache.add(uuid, "node-1");
        System.out.println("cache = " + cache);
        assert 1 == cache.size();
        String val=cache.get(uuid);
        assert Objects.equals(val, "node-1");

        cache.remove(uuid);
        System.out.println("cache = " + cache);
    }

    public static void testRemoveAndAdd() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<>();
        UUID uuid=UUID.randomUUID();
        cache.add(uuid, "val");
        cache.remove(uuid);
        assert cache.size() == 1;
        String val=cache.get(uuid);
        assert val.equals("val");

        cache.add(uuid, "val2");
        val=cache.get(uuid);
        assert val.equals("val2");
    }

    public static void testRemoveAll() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<>(10, 0);
        List<UUID> list=Arrays.asList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        int cnt=1;
        for(UUID uuid: list)
            cache.add(uuid, "node-" + cnt++);
        UUID uuid1=UUID.randomUUID();
        UUID uuid2=UUID.randomUUID();
        cache.add(uuid1, "foo");
        cache.add(uuid2, "bar");
        System.out.println("cache = " + cache);
        assert cache.size() == 5;

        System.out.println("removing " + list);
        cache.removeAll(list);
        System.out.println("cache = " + cache);
        assert cache.size() == 5;
        assert cache.get(uuid1).equals("foo");
        assert cache.get(uuid2).equals("bar");

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 2;
        assert cache.get(uuid1).equals("foo");
        assert cache.get(uuid2).equals("bar");
    }

    public static void testRetainAll() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<>(10, 0);
        List<UUID> list=Arrays.asList(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
        int cnt=1;
        for(UUID uuid: list)
            cache.add(uuid, "node-" + cnt++);
        UUID uuid1=UUID.randomUUID();
        UUID uuid2=UUID.randomUUID();
        cache.add(uuid1, "foo");
        cache.add(uuid2, "bar");
        System.out.println("cache = " + cache);
        assert cache.size() == 5;

        List<UUID> retain=Arrays.asList(uuid1, uuid2);
        System.out.println("retaining " + retain);
        cache.retainAll(retain);
        System.out.println("cache = " + cache);
        assert cache.size() == 5;
        assert cache.get(uuid1).equals("foo");
        assert cache.get(uuid2).equals("bar");

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 2;
        assert cache.get(uuid1).equals("foo");
        assert cache.get(uuid2).equals("bar");
    }


    public static void testRemovalOnExceedingMaxSize() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<>(2, 0);
        UUID u1=UUID.randomUUID(), u2=UUID.randomUUID(), u3=UUID.randomUUID(), u4=UUID.randomUUID();
        cache.add(u1, "u1"); cache.add(u2, "u2");
        assert cache.size() == 2;
        cache.add(u3, "u3"); cache.add(u4, "u4");
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove(u3);
        System.out.println("cache = " + cache);
        assert cache.size() == 3;

        cache.remove(u1);
        System.out.println("cache = " + cache);
        assert cache.size() == 2;

        cache.remove(u4);
        System.out.println("cache = " + cache);
        assert cache.size() == 2;

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 1;
    }


    public static void testRemovalOnExceedingMaxSizeAndMaxTime() {
        LazyRemovalCache<Address, String> cache=new LazyRemovalCache<>(2, 1000);
        Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B"),
          c=Util.createRandomAddress("C"), d=Util.createRandomAddress("D");
        cache.add(a, "A"); cache.add(b, "B");
        assert cache.size() == 2;
        cache.add(c, "C"); cache.add(d, "D");
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove(c);
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove(a);
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove(d);
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        Util.sleep(1100);
        cache.remove(d);
        System.out.println("cache = " + cache);
        assert cache.size() == 1;
    }

    public void testValuesIterator() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<>(10, 10000);
        UUID u1=UUID.randomUUID(), u2=UUID.randomUUID(), u3=UUID.randomUUID(), u4=UUID.randomUUID();
        cache.add(u1, "u1"); cache.add(u2, "u2");
        cache.add(u3, "u3"); cache.add(u4, "u4");
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        int count=0;
        for(LazyRemovalCache.Entry<String> entry: cache.valuesIterator()) {
            System.out.println(entry);
            count++;
        }
        assert count == 4;
    }


}
