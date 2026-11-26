
package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.ViewId;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * Author: Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class ViewIdTest {
    private ViewId v1, v2, v3, v4;

    @BeforeClass
    void setUp() throws UnknownHostException {
        v1=new ViewId(Util.createRandomAddress("A"), 22);
        v2=new ViewId(Util.createRandomAddress("B"), 21);
        v3=v1.copy();
        v4=new ViewId(Util.createRandomAddress("C"), 22);
    }


    public void test0() {
        assert v1.equals(v2) == false;
    }

    public void test1() {
        assert v1.equals(v3) : "v1 and v3 should be the same view";
    }


    public void testCopy() {
        ViewId tmp=v1.copy();
        assert v1.equals(tmp);
    }


    public void testCompareTo() {
        assert v1.compareTo(v3) == 0;
    }


    public void testCompareTo2() {
        assert v1.compareTo(v2) > 0;
    }


    public void testCompareTo3() {
        assert v2.compareTo(v1) < 0;
    }

    public void testCompareToWithSameID() {
        int expected=v4.getCreator().compareTo(v1.getCreator());
        assert v4.compareTo(v1) == expected; // we're comparing IDs (same) and then creators
        assert v4.compareToIDs(v1) == 0;     // we're only comparing IDs
    }

    public void testHashCode() {
        Map<ViewId,Integer> map=new HashMap<>();
        map.put(v1, 1);
        assert map.size() == 1;

        map.put(v2, 2);
        assert map.size() == 2;

        map.put(v3, 3);
        assert map.size() == 2; // replaces v1

        map.put(v4, 4);
        System.out.println("map = " + map);
        assert map.size() == 3; // v1 and v3 are the same

        assert map.get(v1) == 3;
        assert map.get(v2) == 2;
        assert map.get(v3) == 3;
        assert map.get(v4) == 4;
    }
   
}


