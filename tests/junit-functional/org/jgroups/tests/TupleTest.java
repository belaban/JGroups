package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.util.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: TupleTest.java,v 1.1.2.2 2008/01/21 11:36:17 belaban Exp $
 */
public class TupleTest extends TestCase {


    public void testCreation() {
        Tuple<String,Integer> tuple=new Tuple<String,Integer>("Bela", 322649);
        System.out.println("tuple: " + tuple);
        assertEquals("Bela", tuple.getVal1());
        assertEquals(322649, tuple.getVal2().intValue());
    }


    public void testSet() {
        Tuple<String,Integer> tuple=new Tuple<String,Integer>("Bela", 322649);
        System.out.println("tuple: " + tuple);
        tuple.setVal1("Michelle");
        tuple.setVal2(7);
        assertEquals("Michelle", tuple.getVal1());
        assertEquals(7, tuple.getVal2().intValue());
    }

    public void testHashMap() {
        Map<Integer,Tuple<String,Integer>> map=new HashMap<Integer,Tuple<String,Integer>>();

        map.put(1, new Tuple<String,Integer>("one",1));
        map.put(2, new Tuple<String,Integer>("two", 2));
        System.out.println("map: " + map);
        assertEquals("one", map.get(1).getVal1());
        assertEquals(1, map.get(1).getVal2().intValue());
        assertEquals("two", map.get(2).getVal1());
        assertEquals(2, map.get(2).getVal2().intValue());
    }
}
