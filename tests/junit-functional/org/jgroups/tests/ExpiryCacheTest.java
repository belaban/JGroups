package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.ExpiryCache;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups=Global.FUNCTIONAL)
public class ExpiryCacheTest {

    public void testAdd() {
        ExpiryCache<String> cache=new ExpiryCache<>(10000);
        boolean added=add(cache, "Bela");
        assert added;

        added=add(cache, "Michelle");
        assert added;

        added=add(cache, "Nicole");
        assert added;

        System.out.println("cache = " + cache);
        assert cache.size() == 3;
        added=add(cache, "Bela");
        assert !added;
        assert cache.size() == 3;
    }


    public void testReplaceExpiredElement() {
        ExpiryCache<String> cache=new ExpiryCache<>(200);
        add(cache, "Bela");
        for(int i=0; i < 20; i++) {
            if(cache.hasExpired("Bela"))
                break;
            Util.sleep(500);
        }
        assert cache.hasExpired("Bela");

        boolean added=cache.addIfAbsentOrExpired("Bela");
        assert added : "cache is " + cache;
    }


    public void testRemovedExpiredElements() {
        ExpiryCache<String> cache=new ExpiryCache<>(200);
        add(cache, "Bela");
        add(cache, "Michelle");
        add(cache, "Nicole");
        assert cache.size() == 3;
        for(int i=0; i < 20; i++) {
            if(cache.removeExpiredElements() > 0 && cache.size() == 0)
                break;
            Util.sleep(500);
        }
        assert cache.size() == 0;
    }


    protected static <T> boolean add(ExpiryCache<T> cache, T key) {
        boolean added=cache.addIfAbsentOrExpired(key);
        System.out.println((added? "added " : "didn't add ") + key);
        return added;
    }
}
