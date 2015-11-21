package org.jgroups.blocks;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.*;
import java.util.*;

public class CacheTest {

    private static final String KEY1 = "key1";
    private static final String VALUE1 = "value1";
    private static final String KEY2 = "key2";
    private static final String VALUE2 = "value2";
    private static final String KEY3 = "key3";
    private static final String VALUE3 = "value3";

    @Test
    public void testDefaultConstructor() {

        // Run
        Cache<String, String> cache = new Cache<>();

        // Verify
        Assert.assertFalse(cache.isReapingEnabled());
        Assert.assertEquals(cache.getSize(), 0);
        Assert.assertTrue(cache.entrySet().isEmpty());
        Assert.assertEquals(cache.getMaxNumberOfEntries(), 0);
    }

    @Test
    public void testGetValue() throws Exception {

        // Setup
        Cache<String, String> cache = new Cache<>();
        cache.put(KEY1, VALUE1, 60000);
        cache.put(KEY2, VALUE2, 10);

        // Wait for KEY2 to expire before getting the values
        Thread.sleep(100);
        String value1 = cache.get(KEY1);
        String value2 = cache.get(KEY2);

        Assert.assertEquals(value1, VALUE1);
        Assert.assertTrue(cache.getInternalMap().containsKey(KEY1));

        // KEY2 should have been evicted and get() should return null
        Assert.assertNull(value2);
        Assert.assertFalse(cache.getInternalMap().containsKey(KEY2));
    }

    @Test
    public void testRemoveValue() throws Exception {

        // Setup
        Cache<String, String> cache = new Cache<>();
        cache.put(KEY1, VALUE1, 10000);

        // Run
        String value1 = cache.remove(KEY1);

        // Verify
        Assert.assertEquals(value1, VALUE1);
        Assert.assertNull(cache.getEntry(KEY1));
    }

    @Test
    public void testReaping() throws Exception {

        // Setup
        Cache<String, String> cache = new Cache<>();
        TestChangeListener listener = new TestChangeListener(cache);
        cache.addChangeListener(listener);
        cache.enableReaping(100);
        cache.start();

        // Run
        cache.put(KEY1, VALUE1, 250);
        cache.put(KEY2, VALUE2, 500);
        Thread.sleep(750);
        cache.stop();

        // Verify
        Assert.assertEquals(listener.changes.size(), 2);
        Assert.assertEquals(listener.changes.get(0).size, 1);
        Assert.assertEquals(listener.changes.get(0).entries.iterator().next().getKey(), KEY2);
        Assert.assertEquals(listener.changes.get(1).size, 0);
        Assert.assertEquals(cache.getSize(), 0);
    }

    @Test
    public void testChangeReapInterval() throws Exception {

        // Setup
        Cache<String, String> cache = new Cache<>();
        cache.enableReaping(10000);
        cache.start();

        // Run
        cache.put(KEY1, VALUE1, 200);
        cache.enableReaping(50);
        Thread.sleep(300);
        cache.stop();

        // Verify
        Assert.assertEquals(cache.getSize(), 0);
    }

    @Test
    public void testDisableReaping() throws Exception {

        // Setup
        Cache<String, String> cache = new Cache<>();
        cache.enableReaping(500);
        cache.start();

        // Run
        cache.put(KEY1, VALUE1, 200);
        cache.disableReaping();
        Thread.sleep(500);
        cache.stop();

        // Verify
        Assert.assertFalse(cache.isReapingEnabled());
        Assert.assertEquals(cache.getSize(), 1);
        Assert.assertNotNull(cache.getEntry(KEY1));
    }

    @Test
    public void testStopAndRestart() throws Exception {

        // Setup
        Cache<String, String> cache = new Cache<>();
        cache.start();
        cache.enableReaping(500);

        // Run
        cache.put(KEY1, VALUE1, 100);

        // Stop the cache before reaping takes place
        cache.stop();

        // Wait long enough for reaping to have taken place
        Thread.sleep(800);

        // Verify the entry was not evicted
        Assert.assertNotNull(cache.getEntry(KEY1));

        // Restart and re-enable reaping
        cache.start();
        cache.enableReaping(100);
        Thread.sleep(300);

        // Verify entry was evicted
        Assert.assertNull(cache.getEntry(KEY1));
    }

    @Test
    public void testMaxEntries() throws Exception {
        // Setup
        Cache<String, String> cache = new Cache<>();
        cache.setMaxNumberOfEntries(2);

        // Run
        cache.put(KEY1, VALUE1, 100);
        cache.put(KEY2, VALUE2, 5000);

        // Wait long enough for KEY1 to expire
        Thread.sleep(200);

        // Verify KEY1 is still in the cache
        Assert.assertNotNull(cache.getEntry(KEY1));

        // Add a third entry which exceeds the maximum number of entries
        cache.put(KEY3, VALUE3, 5000);

        // Wait for eviction to run
        Thread.sleep(500);

        // Verify KEY1 was evicted because it was expired
        Assert.assertNull(cache.getEntry(KEY1));

        // Verify the cache still contains 2 entries (KEY2 and KEY3)
        Assert.assertEquals(cache.getSize(), 2);

        // Add KEY1 back in so that the oldest entry (KEY2) is evicted
        cache.put(KEY1, VALUE1, 5000);

        // Wait for eviction to run
        Thread.sleep(500);

        // Verify KEY2 was evicted because it was the oldest
        Assert.assertNull(cache.getEntry(KEY2));

        // Verify the cache still contains 2 entries (KEY1 and KEY3)
        Assert.assertEquals(cache.getSize(), 2);
    }

    @Test
    public void testRemoveListener() throws Exception {

        // Setup
        Cache<String, String> cache = new Cache<>();
        TestChangeListener listener = new TestChangeListener(cache);
        cache.addChangeListener(listener);
        cache.enableReaping(100);
        cache.start();

        // Run
        cache.put(KEY1, VALUE1, 200);
        cache.put(KEY2, VALUE2, 800);
        Thread.sleep(500);
        cache.removeChangeListener(listener);
        Thread.sleep(500);
        cache.stop();

        // Listener should only have detected the eviction of KEY1
        Assert.assertEquals(listener.changes.size(), 1);

        // KEY2 should be in the cache after the first eviction
        Assert.assertEquals(listener.changes.get(0).size, 1);
        Assert.assertEquals(listener.changes.get(0).entries.iterator().next().getKey(), KEY2);
    }

    @Test
    public void testListenerThrowsException() throws Exception {

        // Setup
        Cache<String, String> cache = new Cache<>();

        // Listeners are notified in the order they are added, so add the bad listener first
        // to ensure we throw an exception before the good listener is notified of changes
        TestChangeListener goodListener = new TestChangeListener(cache);
        cache.addChangeListener(new BadChangeListener());
        cache.addChangeListener(goodListener);
        cache.enableReaping(100);
        cache.start();

        // Run
        cache.put(KEY1, VALUE1, 200);
        Thread.sleep(400);
        cache.stop();

        // Good listener should have been notified that KEY1 was evicted
        Assert.assertEquals(goodListener.changes.size(), 1);
    }

    @Test
    public void testDumpAndToString() {

        // Setup
        Cache<String, String> cache = new Cache<>();
        cache.put(KEY1, VALUE1, 1000);
        cache.put(KEY2, VALUE2, 0);

        // Run
        String dump = cache.dump();
        String toString = cache.toString();

        // Verify
        Assert.assertTrue(dump.contains("key1: value1\n"));
        Assert.assertTrue(dump.contains("key2: value2\n"));
        Assert.assertTrue(toString.contains("key1: value1 (expiration_time: "));
        Assert.assertTrue(toString.contains("key2: value2 (expiration_time: 0)"));
    }

    @Test
    public void testDumpWithBytes() {

        // Setup
        Cache<String, byte[]> cache = new Cache<>();
        cache.put(KEY1, VALUE1.getBytes(), 1000);
        cache.put(KEY2, VALUE2.getBytes(), 0);

        // Run
        String dump = cache.dump();

        // Verify
        Assert.assertTrue(dump.contains("key1:  (6 bytes)\n"));
        Assert.assertTrue(dump.contains("key2:  (6 bytes)\n"));
    }

    @Test
    public void testValueSerialization() throws Exception {

        // Setup
        Cache.Value<String> original = new Cache.Value<>(VALUE1, 1000);
        Cache.Value<String> copy = new Cache.Value<>();

        // Write the original to a ByteArrayOutputStream
        ByteArrayOutputStream valueAsBytes = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(valueAsBytes);
        original.writeExternal(out);
        out.flush();

        // Read the copy from a ByteArrayInputStream
        long insertionTimeMin = System.currentTimeMillis();
        ObjectInput in = new ObjectInputStream(new ByteArrayInputStream(valueAsBytes.toByteArray()));
        copy.readExternal(in);
        long insertionTimeMax = System.currentTimeMillis();

        // Verify original and copy have the same values
        Assert.assertEquals(copy.getValue(), original.getValue());
        Assert.assertEquals(copy.getTimeout(), original.getTimeout());
        Assert.assertTrue(copy.getInsertionTime() >= insertionTimeMin);
        Assert.assertTrue(copy.getInsertionTime() <= insertionTimeMax);
    }

    private static final class CacheState {

        final long timestamp;
        final Set<Map.Entry> entries;
        final int size;
        final boolean isReaping;

        public CacheState(Set<Map.Entry> entries, int size, boolean isReaping) {
            this.timestamp = System.nanoTime();
            this.size = size;
            this.isReaping = isReaping;
            this.entries = new HashSet<>(entries);
        }
    }

    private static final class TestChangeListener implements Cache.ChangeListener {

        final Cache cache;
        final List<CacheState> changes = new ArrayList<>();

        public TestChangeListener(Cache cache) {
            this.cache = cache;
        }

        @Override
        public void changed() {
           changes.add(new CacheState(cache.entrySet(), cache.getSize(), cache.isReapingEnabled()));
        }
    }

    private static final class BadChangeListener implements Cache.ChangeListener {

        @Override
        public void changed() {
            throw new RuntimeException("Test exception");
        }
    }
}
