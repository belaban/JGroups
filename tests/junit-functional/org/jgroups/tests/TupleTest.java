package org.jgroups.tests;

import org.jgroups.util.Tuple;
import org.jgroups.Global;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class TupleTest {

    public static void testCreation() {
        Tuple<String,Integer> tuple=new Tuple<>("Bela", 322649);
        System.out.println("tuple: " + tuple);
        Assert.assertEquals("Bela", tuple.getVal1());
        Assert.assertEquals(322649, tuple.getVal2().intValue());
    }



    public static void testHashMap() {
        Map<Integer,Tuple<String,Integer>> map=new HashMap<>();
        map.put(1, new Tuple<>("one",1));
        map.put(2, new Tuple<>("two", 2));
        System.out.println("map: " + map);
        Assert.assertEquals("one", map.get(1).getVal1());
        Assert.assertEquals(1, map.get(1).getVal2().intValue());
        Assert.assertEquals("two", map.get(2).getVal1());
        Assert.assertEquals(2, map.get(2).getVal2().intValue());
    }
}
