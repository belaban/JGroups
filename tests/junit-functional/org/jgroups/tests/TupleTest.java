package org.jgroups.tests;

import org.jgroups.util.Tuple;
import org.jgroups.Global;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: TupleTest.java,v 1.2 2008/03/10 15:39:22 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL)
public class TupleTest {

    public static void testCreation() {
        Tuple<String,Integer> tuple=new Tuple<String,Integer>("Bela", 322649);
        System.out.println("tuple: " + tuple);
        Assert.assertEquals("Bela", tuple.getVal1());
        Assert.assertEquals(322649, tuple.getVal2().intValue());
    }

    public static void testSet() {
        Tuple<String,Integer> tuple=new Tuple<String,Integer>("Bela", 322649);
        System.out.println("tuple: " + tuple);
        tuple.setVal1("Michelle");
        tuple.setVal2(7);
        Assert.assertEquals("Michelle", tuple.getVal1());
        Assert.assertEquals(7, tuple.getVal2().intValue());
    }

    public static void testHashMap() {
        Map<Integer,Tuple<String,Integer>> map=new HashMap<Integer,Tuple<String,Integer>>();

        map.put(1, new Tuple<String,Integer>("one",1));
        map.put(2, new Tuple<String,Integer>("two", 2));
        System.out.println("map: " + map);
        Assert.assertEquals("one", map.get(1).getVal1());
        Assert.assertEquals(1, map.get(1).getVal2().intValue());
        Assert.assertEquals("two", map.get(2).getVal1());
        Assert.assertEquals(2, map.get(2).getVal2().intValue());
    }
}
