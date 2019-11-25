package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.SuppressCache;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Arrays;

/**
 * Tests SuppressCache
 * @author Bela Ban
 * @since 3.2
 */
@Test(groups=Global.FUNCTIONAL)
public class SuppressCacheTest {


    public void testPut() {
        SuppressCache<String> cache=new SuppressCache<>();

        SuppressCache.Value val=cache.putIfAbsent("Bela", 5000);
        System.out.println("cache = " + cache);
        assert val.count() == 1;

        val=cache.putIfAbsent("Bela", 5000);
        System.out.println("cache = " + cache);
        assert val == null; // already present


        val=cache.putIfAbsent("Michelle", 5000);
        assert val.count() == 1;

        for(int i=0; i < 5; i++) {
            val=cache.putIfAbsent("Michelle", 5000);
            assert val == null;
        }

        Util.sleep(2000);
        System.out.println("cache:\n" + cache);

        val=cache.putIfAbsent("Bela", 500);
        assert val.count() == 3;

        val=cache.putIfAbsent("Bela", 5000);
        assert val == null;
    }


    public void testNullKey() {
        SuppressCache<String> cache=new SuppressCache<>();
        cache.putIfAbsent(null, 10);

        cache.removeAll(Arrays.asList("Bela", "Michi"));
        Util.sleep(500);
        cache.removeExpired(10);

        System.out.println("cache = " + cache);


    }
}
