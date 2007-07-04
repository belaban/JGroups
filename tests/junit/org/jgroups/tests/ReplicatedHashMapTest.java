package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.util.Util;
import org.jgroups.blocks.ReplicatedHashMap;

import java.util.HashMap;
import java.util.Map;

/**
 * Test methods for ReplicatedHashMap
 * @author Bela Ban
 * @version $Id: ReplicatedHashMapTest.java,v 1.1 2007/07/04 14:30:59 belaban Exp $
 **/
public class ReplicatedHashMapTest extends ChannelTestBase {

    private static int testCount = 1;

    private ReplicatedHashMap<String,String> map1;
    private ReplicatedHashMap<String,String> map2;

    public ReplicatedHashMapTest(String testName) {
        super(testName);
    }

    protected void setUp() throws Exception {
        super.setUp();
        System.out.println("#### Setup Test " + testCount);

        Channel c1=createChannel("A");
        this.map1=new ReplicatedHashMap<String,String>(c1, false, 5000);
        map1.setBlockingUpdates(true);
        c1.connect("demo");
        this.map1.start(5000);

        Channel c2=createChannel("A");
        this.map2=new ReplicatedHashMap<String,String>(c2, false, 5000);
        map2.setBlockingUpdates(true);
        c2.connect("demo");
        this.map2.start(5000);
    }

    protected void tearDown() throws Exception {
        this.map1.stop();
        this.map2.stop();
        System.out.println("#### TearDown Test " + testCount + "\n\n");
        testCount++;
        super.tearDown();
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

    public void testPutAll() {
        Map<String,String> all1 = new HashMap<String,String>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2 = new HashMap<String,String>();
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
        Map<String,String> all1 = new HashMap<String,String>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2 = new HashMap<String,String>();
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
        Map<String,String> all1 = new HashMap<String,String>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2 = new HashMap<String,String>();
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