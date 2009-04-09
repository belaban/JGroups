package org.jgroups.blocks;

import org.testng.annotations.Test;
import org.jgroups.Global;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;

import java.util.Arrays;
import java.util.List;

/**
 * @author Bela Ban
 * @version $Id: LazyRemovalCacheTest.java,v 1.2 2009/04/09 09:11:36 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL,sequential=false)
public class LazyRemovalCacheTest {

    public static void testAdd() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<UUID, String>();
        UUID uuid=UUID.randomUUID();
        cache.add(uuid, "node-1");
        System.out.println("cache = " + cache);
        assert 1 == cache.size();
        String val=cache.get(uuid);
        assert val != null && val.equals("node-1");

        cache.remove(uuid);
        System.out.println("cache = " + cache);
    }

    public static void testRemoveAll() {
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<UUID, String>(10, 0);
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
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<UUID, String>(10, 0);
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
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<UUID, String>(2, 0);
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
        LazyRemovalCache<UUID, String> cache=new LazyRemovalCache<UUID, String>(2, 1000);
        UUID u1=UUID.randomUUID(), u2=UUID.randomUUID(), u3=UUID.randomUUID(), u4=UUID.randomUUID();
        cache.add(u1, "u1"); cache.add(u2, "u2");
        assert cache.size() == 2;
        cache.add(u3, "u3"); cache.add(u4, "u4");
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove(u3);
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove(u1);
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.remove(u4);
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        cache.removeMarkedElements();
        System.out.println("cache = " + cache);
        assert cache.size() == 4;

        System.out.println("sleeping for 1 sec");
        Util.sleep(1100);
        cache.remove(u4);
        System.out.println("cache = " + cache);
        assert cache.size() == 1;

    }


}
