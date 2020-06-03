package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.util.IntHashMap;
import org.testng.annotations.Test;

/**
 * Tests {@link IntHashMap}
 * @author Bela Ban
 * @since  5.0.0
 */
@Test(groups=Global.FUNCTIONAL)
public class IntHashMapTest {

    public void testConstructor() {
        IntHashMap<String> map=new IntHashMap<>(5);
        assert map.getCapacity() == 6;
        map=new IntHashMap<>(0);
        assert map.getCapacity() == 1;

        try {
            map=new IntHashMap<>(-1);
            assert false : "should have caught exception";
        }
        catch(IllegalArgumentException ex) {
            System.out.printf("caught exception as expected: %s\n", ex);
        }
    }

    public void testPut() {
        IntHashMap<String> map=new IntHashMap<>(20);
        assert map.isEmpty();
        map.put(0, "Bela");
        System.out.println("map = " + map);
        assert map.size() == 1;

        map.put(4, "Michi");
        assert map.size() == 2;

        try {
            map.put(0, "Bela Ban"); // duplicate entry
            assert false : "duplicate put";
        }
        catch(IllegalStateException ex) {
            System.out.printf("caught exception, as expected: %s\n", ex);
        }
    }

    public void testPutWithCapacityIncrease() {
        IntHashMap<String> map=new IntHashMap<>(4);
        assert map.getCapacity() == 5;
        map.put(3, "hello").put(4, "world");
        map.put(5, "boom");
        assert map.getCapacity() == 6;
    }

    public void testContainsKey() {
        IntHashMap<String> map=new IntHashMap<>(4);
        map.put(3, "hello").put(4, "world");
        assert map.containsKey(4);
        assert !map.containsKey(5);
        assert !map.containsKey(2);
    }

    public void testGet() {
        IntHashMap<String> map=new IntHashMap<>(4);
        map.put(3, "hello").put(4, "world");
        assert map.get(3).equals("hello");
        assert map.get(4).equals("world");
        assert map.get(2) == null;
        assert map.get(10) == null;
    }

}
