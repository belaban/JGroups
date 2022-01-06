package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.blocks.ReplicatedHashMap;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test methods for ReplicatedHashMap
 * @author Bela Ban
 */
@Test(groups={Global.STACK_DEPENDENT,Global.EAP_EXCLUDED},singleThreaded=true)
public class ReplicatedHashMapTest extends ChannelTestBase {
    private ReplicatedHashMap<String,String>       map1;
    private ReplicatedHashMap<String,String>       map2;
    private final ConcurrentHashMap<String,String> wrap=new ConcurrentHashMap<>();

    @BeforeClass
    protected void setUp() throws Exception {
        JChannel c1=createChannel();
        this.map1=new ReplicatedHashMap<>(c1);
        map1.setBlockingUpdates(true);
        JChannel c2=createChannel();
        this.map2=new ReplicatedHashMap<>(wrap, c2);
        map2.setBlockingUpdates(true);
        makeUnique(c1,c2);

        c1.connect("ReplicatedHashMapTest");
        this.map1.start(5000);

        c2.connect("ReplicatedHashMapTest");
        this.map2.start(5000);
    }

    @AfterMethod
    protected void clean() {
        map1.clear();
        map2.clear();
    }

    @AfterClass
    protected void tearDown() throws Exception {
        this.map1.stop();
        this.map2.stop();
    }

    public void testEqualsEtc() {
        map1.put("key1", "value1");
        assert this.map1.equals(this.map2);
        Assert.assertEquals(this.map1.hashCode(), this.map2.hashCode());
        Assert.assertEquals(this.map1.toString(), this.map2.toString());
        assert this.wrap.equals(this.map1);
    }

    public void testSize() {
        Assert.assertEquals(0, this.map1.size());
        Assert.assertEquals(this.map2.size(), this.map1.size());

        this.map1.put("key1", "value1");
        Assert.assertEquals(1, this.map1.size());
        Assert.assertEquals(this.map2.size(), this.map1.size());

        this.map2.put("key2", "value2");
        Assert.assertEquals(2, this.map1.size());
        Assert.assertEquals(this.map2.size(), this.map1.size());
    }

    public void testIsEmpty() {
        assert this.map1.isEmpty();
        assert this.map2.isEmpty();

        this.map1.put("key", "value");

        assert !this.map1.isEmpty();
        assert !this.map2.isEmpty();
    }

    public void testContainsKey() {
        assert !this.map1.containsKey("key1");
        assert !this.map2.containsKey("key1");
        this.map1.put("key1", "value");
        assert this.map1.containsKey("key1");
        assert this.map2.containsKey("key1");
        this.map2.put("key2", "value");
        assert this.map1.containsKey("key2");
        assert this.map2.containsKey("key2");
    }

    public void testContainsValue() {
        assert !this.map1.containsValue("value1");
        assert !this.map2.containsValue("value1");
        this.map1.put("key1", "value1");
        assert this.map1.containsValue("value1");
        assert this.map2.containsValue("value1");
        this.map2.put("key2", "value2");
        assert this.map1.containsValue("value2");
        assert this.map2.containsValue("value2");
    }

    public void testPutAndGet() {
        assert this.map1.get("key1") == null;
        assert this.map2.get("key1") == null;
        this.map1.put("key1", "value1");
        assert this.map1.get("key1") != null;
        assert this.map2.get("key1") != null;
        this.map2.put("key2", "value2");
        assert this.map1.get("key2") != null;
        assert this.map2.get("key2") != null;
    }

    public void testPutIfAbsent() {
        String retval=map1.putIfAbsent("name", "Bela");
        assert retval == null;
        retval=map1.putIfAbsent("name", "Michelle");
        assert retval != null;
        Assert.assertEquals("Bela", retval);
        Assert.assertEquals("Bela", map1.get("name"));
        Assert.assertEquals("Bela", map2.get("name"));
    }

    public void testRemove() {
        assert this.map1.get("key1") == null;
        assert this.map2.get("key1") == null;
        this.map1.put("key1", "value1");
        this.map2.put("key2", "value2");
        assert this.map1.get("key1") != null;
        assert this.map2.get("key1") != null;
        assert this.map1.get("key2") != null;
        assert this.map2.get("key2") != null;

        this.map1.remove("key1");
        assert this.map1.get("key1") == null;
        assert this.map2.get("key1") == null;
        assert this.map1.get("key2") != null;
        assert this.map2.get("key2") != null;

        this.map2.remove("key2");
        assert this.map1.get("key2") == null;
        assert this.map2.get("key2") == null;
    }

    public void testRemove2() {
        map1.put("name", "Bela");
        map1.put("id", "322649");
        System.out.println("map1: " + map1);
        boolean removed=map1.remove("id", "322000");
        assert !removed;
        assert map1.containsKey("id");
        removed=map1.remove("id", "322649");
        System.out.println("map1: " + map1);
        assert removed;
        assert !map1.containsKey("id");
        Assert.assertEquals(1, map2.size());
    }

    public void testReplace() {
        map1.put("name", "Bela");
        map1.put("id", "322649");
        System.out.println("map1: " + map1);
        String val=map1.replace("id2", "322000");
        Assert.assertEquals(2, map1.size());
        assert map1.get("id2") == null;
        System.out.println("map1: " + map1);
        assert val == null;
        val=map1.replace("id", "322000");
        System.out.println("map1: " + map1);
        assert val != null;
        Assert.assertEquals("322649", val);
        Assert.assertEquals("322000", map1.get("id"));
        Assert.assertEquals("322000", map2.get("id"));
    }

    public void testReplace2() {
        map1.put("name", "Bela");
        map1.put("id", "322649");
        System.out.println("map1: " + map1);
        boolean replaced=map1.replace("id", "322000", "1");
        assert !replaced;
        Assert.assertEquals("322649", map1.get("id"));
        replaced=map1.replace("id", "322649", "1");
        assert replaced;
        Assert.assertEquals("1", map1.get("id"));
    }

    public void testPutAll() {
        Map<String,String> all1=new HashMap<>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2=new HashMap<>();
        all2.put("key3", "value3");
        all2.put("key4", "value4");

        this.map1.putAll(all1);
        Assert.assertEquals(2, this.map1.size());
        Assert.assertEquals(2, this.map2.size());
        this.map2.putAll(all2);
        Assert.assertEquals(4, this.map1.size());
        Assert.assertEquals(4, this.map2.size());

        assert this.map1.containsKey("key1");
        assert this.map1.containsKey("key2");
        assert this.map1.containsKey("key3");
        assert this.map1.containsKey("key4");

        assert this.map2.containsKey("key1");
        assert this.map2.containsKey("key2");
        assert this.map2.containsKey("key3");
        assert this.map2.containsKey("key4");
    }

    public void testClear() {
        assert this.map1.isEmpty();
        assert this.map2.isEmpty();

        this.map1.put("key", "value");
        assert !this.map1.isEmpty();
        assert !this.map2.isEmpty();

        this.map1.clear();
        assert this.map1.isEmpty();
        assert this.map2.isEmpty();
        this.map2.put("key", "value");
        assert !this.map1.isEmpty();
        assert !this.map2.isEmpty();

        this.map2.clear();
        assert this.map1.isEmpty();
        assert this.map2.isEmpty();
    }

    public void testKeySet() {
        Map<String,String> all1=new HashMap<>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2=new HashMap<>();
        all2.put("key3", "value3");
        all2.put("key4", "value4");

        this.map1.putAll(all1);
        assert all1.keySet().equals(this.map1.keySet());
        assert all1.keySet().equals(this.map2.keySet());

        this.map2.putAll(all2);
        all1.putAll(all2);
        assert all1.keySet().equals(this.map1.keySet());
        assert all1.keySet().equals(this.map2.keySet());
    }

    public void testKeySet_mutate() {
        Map<String,String> all1=new HashMap<>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2=new HashMap<>();
        all2.put("key3", "value3");
        all2.put("key4", "value4");

        this.map1.putAll(all1);
        assert all1.keySet().equals(this.map1.keySet());
        assert all1.keySet().equals(this.map2.keySet());

        this.map2.putAll(all2);
	this.map1.keySet().retainAll(all1.keySet());
        assert all1.keySet().equals(this.map1.keySet());
        assert all1.keySet().equals(this.map2.keySet());
    }

    public void testValues() {
        Map<String,String> all1=new HashMap<>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map<String,String> all2=new HashMap<>();
        all2.put("key3", "value3");
        all2.put("key4", "value4");

        this.map1.putAll(all1);
        assert this.map1.values().containsAll(all1.values());
        assert this.map2.values().containsAll(all1.values());

        this.map2.putAll(all2);
        all1.putAll(all2);
        assert this.map1.values().containsAll(all1.values());
        assert this.map2.values().containsAll(all1.values());
    }


    public void testValuesClear() {
        Map<String,String> all1=new HashMap<>();
        all1.put("key1", "value1");
        all1.put("key2", "value2");

        this.map1.putAll(all1);
        assert this.map1.values().containsAll(all1.values());
        assert this.map2.values().containsAll(all1.values());

        this.map2.values().clear();
        assert map2.isEmpty();
        assert this.map1.isEmpty();

    }

}
