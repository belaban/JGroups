package org.jgroups.blocks;

import org.testng.Assert;
import org.testng.annotations.Test;

public class CacheTest {

    @Test
    public void testCacheGetValue() {

        Cache<String, String> cache = new Cache();
        cache.put("key", "value", 60000);
        String value = cache.get("key");

        // Both the following assertions fail because Cache.get() compares Value.timeout to System.currentTimeMillis()
        // without considering Value.insertion_time
        Assert.assertEquals(value, "value");
        Assert.assertTrue(cache.getInternalMap().containsKey("key"));
    }
}
