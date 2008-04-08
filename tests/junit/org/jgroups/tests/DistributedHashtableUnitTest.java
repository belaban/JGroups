package org.jgroups.tests;

import org.jgroups.Channel;
import org.jgroups.blocks.DistributedHashtable;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Test methods for DistributedHashtable
 * @author denis.pasek@senacor.com
 * @version $Revision: 1.4 $
 **/
public class DistributedHashtableUnitTest extends ChannelTestBase {

    private static int testCount = 1;   

    private DistributedHashtable map1;
    private DistributedHashtable map2;

  

    @BeforeMethod
    protected void setUp() throws Exception {
        ;
        System.out.println("#### Setup Test " + testCount);

        Channel c1=createChannel("A");
        this.map1=new DistributedHashtable(c1, false, 5000);
        c1.connect("demo");
        this.map1.start(5000);

        Channel c2=createChannel("A");
        this.map2=new DistributedHashtable(c2, false, 5000);
        c2.connect("demo");
        this.map2.start(5000);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        this.map1.stop();
        this.map2.stop();
        System.out.println("#### TearDown Test " + testCount + "\n\n");
        testCount++;
        ;
    }

    @Test
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

    @Test
    public void testIsEmpty() {
        assertTrue(this.map1.isEmpty());
        assertTrue(this.map2.isEmpty());

        this.map1.put("key", "value");

        assertFalse(this.map1.isEmpty());
        assertFalse(this.map2.isEmpty());
    }

    @Test
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

    @Test
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

    @Test
    public void testPutAndGet() {
        assert this.map1.get("key1") == null;
        assert this.map2.get("key1") == null;
        this.map1.put("key1", "value1");
        assertNotNull(this.map1.get("key1"));
        assertNotNull(this.map2.get("key1"));
        this.map2.put("key2", "value2");
        assertNotNull(this.map1.get("key2"));
        assertNotNull(this.map2.get("key2"));
    }

    @Test
    public void testRemove() {
        assert this.map1.get("key1") == null;
        assert this.map2.get("key1") == null;
        this.map1.put("key1", "value1");
        this.map2.put("key2", "value2");
        assertNotNull(this.map1.get("key1"));
        assertNotNull(this.map2.get("key1"));
        assertNotNull(this.map1.get("key2"));
        assertNotNull(this.map2.get("key2"));

        this.map1.remove("key1");
        assert this.map1.get("key1") == null;
        assert this.map2.get("key1") == null;
        assertNotNull(this.map1.get("key2"));
        assertNotNull(this.map2.get("key2"));

        this.map2.remove("key2");
        assert this.map1.get("key2") == null;
        assert this.map2.get("key2") == null;
    }

    @Test
    public void testPutAll() {
        Map all1 = new HashMap();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map all2 = new HashMap();
        all2.put("key3", "value3");
        all2.put("key4", "value4");

        this.map1.putAll(all1);
        Assert.assertEquals(2, this.map1.size());
        Assert.assertEquals(2, this.map2.size());
        this.map2.putAll(all2);
        Assert.assertEquals(4, this.map1.size());
        Assert.assertEquals(4, this.map2.size());

        assertTrue(this.map1.containsKey("key1"));
        assertTrue(this.map1.containsKey("key2"));
        assertTrue(this.map1.containsKey("key3"));
        assertTrue(this.map1.containsKey("key4"));

        assertTrue(this.map2.containsKey("key1"));
        assertTrue(this.map2.containsKey("key2"));
        assertTrue(this.map2.containsKey("key3"));
        assertTrue(this.map2.containsKey("key4"));
    }

    @Test
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

    @Test
    public void testKeySet() {
        Map all1 = new HashMap();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map all2 = new HashMap();
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

    @Test
    public void testValues() {
        Map all1 = new HashMap();
        all1.put("key1", "value1");
        all1.put("key2", "value2");
        Map all2 = new HashMap();
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
