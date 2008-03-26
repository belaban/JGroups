package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.blocks.ReplicatedHashMap;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test methods for ReplicatedHashMap
 * 
 * @author Bela Ban
 * @version $Id: ReplicatedHashMapTest.java,v 1.3 2007/08/22 11:23:28 belaban
 *          Exp $
 */
public class ReplicatedHashMapTest extends ChannelTestBase {
    private ReplicatedHashMap<String,String> map1;
    private ReplicatedHashMap<String,String> map2;
    private ConcurrentHashMap<String,String> wrap=new ConcurrentHashMap<String,String>();

    public ReplicatedHashMapTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
        Channel c1=createChannel("A");
        this.map1=new ReplicatedHashMap<String,String>(c1, false);
        map1.setBlockingUpdates(true);
        c1.connect("demo");
        this.map1.start(5000);

        Channel c2=createChannel("A");
        this.map2=new ReplicatedHashMap<String,String>(wrap, c2, false);
        map2.setBlockingUpdates(true);
        c2.connect("demo");
        this.map2.start(5000);
    }

    protected void tearDown() throws Exception {
        this.map1.stop();
        this.map2.stop();
        super.tearDown();
    }

    public void testEqualsEtc() {
        map1.put("key1", "value1");
        assertEquals(this.map1, this.map2);
        assertEquals(this.map1.hashCode(), this.map2.hashCode());
        assertEquals(this.map1.toString(), this.map2.toString());
        assertEquals(this.wrap, this.map1);
    }

    public void testSize() {
        assertEquals(0, this.map1.size());
        assertEquals(this.map2.size(), this.map1.size());

        this.map1.put("key1", "value1");
        assertEquals(1, this.map1.size());
        assertEquals(this.map2.size(), this.map1.size());

        this.map2.put("key2", "value2");
        assertEquals(2, this.map1.size());
        assertEquals(this.map2.size(), this.map1.size());
    }

    //    public void testBla() {
    //        this.map1.put("key1", "value1");
    //        Util.sleep(120000);
    //    }

    public void testIsEmpty() {
        assertTrue(this.map1.isEmpty());
        assertTrue(this.map2.isEmpty());

        this.map1.put("key", "value");

        assertFalse(this.map1.isEmpty());
        assertFalse(this.map2.isEmpty());
    }

    public void testContainsKey() {
        assertFalse(this.map1.containsKey("key1"));
        assertFalse(this.map2.containsKey("key1"));
        this.map1.put("key1", "value");
        assertTrue(this.map1.containsKey("key1"));
        assertTrue(this.map2.containsKey("key1"));
        this.map2.put("key2", "value");
        assertTrue(this.map1.containsKey("key2"));
        assertTrue(this.map2.containsKey("key2"));
    }

    public void testContainsValue() {
        assertFalse(this.map1.containsValue("value1"));
        assertFalse(this.map2.containsValue("value1"));
        this.map1.put("key1", "value1");
        assertTrue(this.map1.containsValue("value1"));
        assertTrue(this.map2.containsValue("value1"));
        this.map2.put("key2", "value2");
        assertTrue(this.map1.containsValue("value2"));
        assertTrue(this.map2.containsValue("value2"));
    }

    public void testPutAndGet() {
        assertNull(this.map1.get("key1"));
        assertNull(this.map2.get("key1"));
        this.map1.put("key1", "value1");
        assertNotNull(this.map1.get("key1"));
        assertNotNull(this.map2.get("key1"));
        this.map2.put("key2", "value2");
        assertNotNull(this.map1.get("key2"));
        assertNotNull(this.map2.get("key2"));
    }

    public void testPutIfAbsent() {
        String retval=map1.putIfAbsent("name", "Bela");
        assertNull(retval);
        retval=map1.putIfAbsent("name", "Michelle");
        assertNotNull(retval);
        assertEquals("Bela", retval);
        assertEquals("Bela", map1.get("name"));
        assertEquals("Bela", map2.get("name"));
    }

    public void testRemove() {
        assertNull(this.map1.get("key1"));
        assertNull(this.map2.get("key1"));
        this.map1.put("key1", "value1");
        this.map2.put("key2", "value2");
        assertNotNull(this.map1.get("key1"));
        assertNotNull(this.map2.get("key1"));
        assertNotNull(this.map1.get("key2"));
        assertNotNull(this.map2.get("key2"));

        this.map1.remove("key1");
        assertNull(this.map1.get("key1"));
        assertNull(this.map2.get("key1"));
        assertNotNull(this.map1.get("key2"));
        assertNotNull(this.map2.get("key2"));

        this.map2.remove("key2");
        assertNull(this.map1.get("key2"));
        assertNull(this.map2.get("key2"));
    }

    public void testRemove2() {
        map1.put("name", "Bela");
        map1.put("id", "322649");
        System.out.println("map1: " + map1);
        boolean removed=map1.remove("id", "322000");
        assertFalse(removed);
        assertTrue(map1.containsKey("id"));
        removed=map1.remove("id", "322649");
        System.out.println("map1: " + map1);
        assertTrue(removed);
        assertFalse(map1.containsKey("id"));
        assertEquals(1, map2.size());
    }

    public void testReplace() {
        map1.put("name", "Bela");
        map1.put("id", "322649");
        System.out.println("map1: " + map1);
        String val=map1.replace("id2", "322000");
        assertEquals(2, map1.size());
        assertNull(map1.get("id2"));
        System.out.println("map1: " + map1);
        assertNull(val);
        val=map1.replace("id", "322000");
        System.out.println("map1: " + map1);
        assertNotNull(val);
        assertEquals("322649", val);
        assertEquals("322000", map1.get("id"));
        assertEquals("322000", map2.get("id"));
    }

    public void testReplace2() {
        map1.put("name", "Bela");
        map1.put("id", "322649");
        System.out.println("map1: " + map1);
        boolean replaced=map1.replace("id", "322000", "1");
        assertFalse(replaced);
        assertEquals("322649", map1.get("id"));
        replaced=map1.replace("id", "322649", "1");
        assertTrue(replaced);
        assertEquals("1", map1.get("id"));
    }

    public void testPutAll() {
        Map<String,String> all1=new HashMap<String,String>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2=new HashMap<String,String>();
        all2.put("key3", "value3");
        all2.put("key4", "value4");

        this.map1.putAll(all1);
        assertEquals(2, this.map1.size());
        assertEquals(2, this.map2.size());
        this.map2.putAll(all2);
        assertEquals(4, this.map1.size());
        assertEquals(4, this.map2.size());

        assertTrue(this.map1.containsKey("key1"));
        assertTrue(this.map1.containsKey("key2"));
        assertTrue(this.map1.containsKey("key3"));
        assertTrue(this.map1.containsKey("key4"));

        assertTrue(this.map2.containsKey("key1"));
        assertTrue(this.map2.containsKey("key2"));
        assertTrue(this.map2.containsKey("key3"));
        assertTrue(this.map2.containsKey("key4"));
    }

    public void testClear() {
        assertTrue(this.map1.isEmpty());
        assertTrue(this.map2.isEmpty());

        this.map1.put("key", "value");
        assertFalse(this.map1.isEmpty());
        assertFalse(this.map2.isEmpty());

        this.map1.clear();
        assertTrue(this.map1.isEmpty());
        assertTrue(this.map2.isEmpty());
        this.map2.put("key", "value");
        assertFalse(this.map1.isEmpty());
        assertFalse(this.map2.isEmpty());

        this.map2.clear();
        assertTrue(this.map1.isEmpty());
        assertTrue(this.map2.isEmpty());
    }

    public void testKeySet() {
        Map<String,String> all1=new HashMap<String,String>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2=new HashMap<String,String>();
        all2.put("key3", "value3");
        all2.put("key4", "value4");

        this.map1.putAll(all1);
        assertEquals(all1.keySet(), this.map1.keySet());
        assertEquals(all1.keySet(), this.map2.keySet());

        this.map2.putAll(all2);
        all1.putAll(all2);
        assertEquals(all1.keySet(), this.map1.keySet());
        assertEquals(all1.keySet(), this.map2.keySet());
    }

    public void testValues() {
        Map<String,String> all1=new HashMap<String,String>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2=new HashMap<String,String>();
        all2.put("key3", "value3");
        all2.put("key4", "value4");

        this.map1.putAll(all1);
        assertTrue(this.map1.values().containsAll(all1.values()));
        assertTrue(this.map2.values().containsAll(all1.values()));

        this.map2.putAll(all2);
        all1.putAll(all2);
        assertTrue(this.map1.values().containsAll(all1.values()));
        assertTrue(this.map2.values().containsAll(all1.values()));
    }

}