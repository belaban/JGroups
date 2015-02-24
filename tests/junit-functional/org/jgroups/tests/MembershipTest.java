
package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Membership;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

/**
 * Author: Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class MembershipTest {
    Membership m1, m2;
    List<Address> v1, v2;
    Address a1, a2, a3, a4, a5;


    @BeforeMethod
    void setUp() {
        a1=Util.createRandomAddress();
        a2=Util.createRandomAddress();
        a3=a2;
        a4=Util.createRandomAddress();
        a5=Util.createRandomAddress();
        m1=new Membership();
    }


    public void testConstructor() {
        v1=Arrays.asList(a1, a2, a3);
        m2=new Membership(v1);
        assert m2.size() == 2;
        assert m2.contains(a1);
        assert m2.contains(a2);
        assert m2.contains(a3);
    }


    public void testClone() {
        v1=Arrays.asList(a1, a2, a3);
        m2=new Membership(v1);
        m1=m2.copy();
        assert m1.size()  == m2.size();
        assert m1.contains(a1);
        assert m1.contains(a2);
        assert m2.contains(a1);
        assert m2.contains(a2);
    }



    public void testCopy() {
        v1=Arrays.asList(a1, a2, a3);
        m2=new Membership(v1);
        m1=m2.copy();
        assert m1.size() == m2.size();
        assert m1.contains(a1);
        assert m1.contains(a2);
        assert m2.contains(a1);
        assert m2.contains(a2);
    }



    public void testAdd() {
        m1.add(a1, a2, a3);
        assert m1.size() == 2;
        assert m1.contains(a1);
        assert m1.contains(a2);
        assert m1.contains(a3);
    }



    public void testAddVector() {
        v1=Arrays.asList(a1, a2, a3);
        m1.add(v1);
        assert m1.size() == 2;
        assert m1.contains(a1);
        assert m1.contains(a2);
    }


    public void testAddVectorDupl() {
        v1=Arrays.asList(a1, a2, a3, a4, a5);
        m1.add(a5, a1).add(v1);
        assert m1.size() == 4;
        assert m1.contains(a1);
        assert m1.contains(a2);
        assert m1.contains(a4);
        assert m1.contains(a5);
    }


    public void testRemove() {
        m1.add(a1, a2, a3, a4, a5).remove(a2);
        assert m1.size() == 3;
    }


    public void testGetMembers() {
        testAdd();
        List<Address> v=m1.getMembers();
        assert v.size() == 2;
    }



    public void testSet() {
        v1=Arrays.asList(a1, a2);
        m1.add(a1, a2, a4, a5).set(v1);
        assert m1.size() == 2;
        assert m1.contains(a1);
        assert m1.contains(a2);
    }



    public void testSet2() {
        m1=new Membership();
        m2=new Membership();
        m1.add(a1, a2, a4);
        m2.add(a5).set(m1);
        assert m2.size() == 3;
        assert m2.contains(a1);
        assert m2.contains(a2);
        assert m2.contains(a4);
        assert !m2.contains(a5);
    }



    public void testMerge() {
        v1=Arrays.asList(a5);
        v2=Arrays.asList(a2, a3);
        m1.add(a1, a2, a3, a4);
        m1.merge(v1, v2);
        assert m1.size() == 3;
        assert m1.contains(a1);
        assert m1.contains(a4);
        assert m1.contains(a5);
    }


    public void testSort() {
        m1.add(a3, a4, a2, a1, a5, a2);
        System.out.println("membership:\n" + printUUIDs(m1));
        Assert.assertEquals(4, m1.size());
        Assert.assertEquals(a3, m1.elementAt(0));
        Assert.assertEquals(a4, m1.elementAt(1));
        Assert.assertEquals(a1, m1.elementAt(2));
        Assert.assertEquals(a5, m1.elementAt(3));
        m1.sort();
        System.out.println("sorted: " + m1);
        Assert.assertEquals(4, m1.size());

        Address addr0=m1.elementAt(0);
        Address addr1=m1.elementAt(1);
        Address addr2=m1.elementAt(2);
        Address addr3=m1.elementAt(3);

        System.out.println("sorted membership:\n" + printUUIDs(m1));
        assert addr0.compareTo(addr1) < 0;
        assert addr1.compareTo(addr2) < 0;
        assert addr2.compareTo(addr3) < 0;
    }


    private static String printUUIDs(Membership mbrs) {
        StringBuilder sb=new StringBuilder();
        boolean first=true;
        for(int i=0; i < mbrs.size(); i++) {
            UUID mbr=(UUID)mbrs.elementAt(i);
            if(first)
                first=false;
            else
                sb.append(", ");
            sb.append(mbr.toStringLong());
        }
        return sb.toString();
    }

}
