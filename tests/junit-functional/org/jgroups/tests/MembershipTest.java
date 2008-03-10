// $Id: MembershipTest.java,v 1.2 2008/03/10 15:39:21 belaban Exp $

package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Membership;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Vector;

@Test(groups=Global.FUNCTIONAL,sequential=true)
public class MembershipTest {
    Membership m1, m2;
    Vector v1, v2;
    Address a1, a2, a3, a4, a5;


    public MembershipTest(String name) {
    }


    @BeforeMethod
    public void setUp() {
        a1=new IpAddress(5555);
        a2=new IpAddress(6666);
        a3=new IpAddress(6666);
        a4=new IpAddress(7777);
        a5=new IpAddress(8888);
        m1=new Membership();
    }

    @AfterMethod
    public void tearDown() {

    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testConstructor() {
        v1=new Vector();
        v1.addElement(a1);
        v1.addElement(a2);
        v1.addElement(a3);
        m2=new Membership(v1);
        assert m2.size() == 2;
        assert m2.contains(a1);
        assert m2.contains(a2);
        assert m2.contains(a3);
    }

    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testClone() {
        v1=new Vector();
        v1.addElement(a1);
        v1.addElement(a2);
        v1.addElement(a3);
        m2=new Membership(v1);
        m1=(Membership)m2.clone();
        Assert.assertEquals(m1.size(), m2.size());
        assert m1.contains(a1);
        assert m1.contains(a2);
        assert m2.contains(a1);
        assert m2.contains(a2);
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testCopy() {
        v1=new Vector();
        v1.addElement(a1);
        v1.addElement(a2);
        v1.addElement(a3);
        m2=new Membership(v1);
        m1=m2.copy();
        Assert.assertEquals(m1.size(), m2.size());
        assert m1.contains(a1);
        assert m1.contains(a2);
        assert m2.contains(a1);
        assert m2.contains(a2);
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testAdd() {
        m1.add(a1);
        m1.add(a2);
        m1.add(a3);
        assert m1.size() == 2;
        assert m1.contains(a1);
        assert m1.contains(a2);
        assert m1.contains(a3);
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testAddVector() {
        v1=new Vector();
        v1.addElement(a1);
        v1.addElement(a2);
        v1.addElement(a3);
        m1.add(v1);
        assert m1.size() == 2;
        assert m1.contains(a1);
        assert m1.contains(a2);
    }

    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testAddVectorDupl() {
        v1=new Vector();
        v1.addElement(a1);
        v1.addElement(a2);
        v1.addElement(a3);
        v1.addElement(a4);
        v1.addElement(a5);

        m1.add(a5);
        m1.add(a1);
        m1.add(v1);
        assert m1.size() == 4;
        assert m1.contains(a1);
        assert m1.contains(a2);
        assert m1.contains(a4);
        assert m1.contains(a5);
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testRemove() {
        m1.add(a1);
        m1.add(a2);
        m1.add(a3);
        m1.add(a4);
        m1.add(a5);
        m1.remove(a2);
        assert m1.size() == 3;
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testGetMembers() {
        testAdd();
        Vector v=m1.getMembers();
        assert v.size() == 2;
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testSet() {
        v1=new Vector();
        v1.addElement(a1);
        v1.addElement(a2);
        m1.add(a1);
        m1.add(a2);
        m1.add(a4);
        m1.add(a5);
        m1.set(v1);
        assert m1.size() == 2;
        assert m1.contains(a1);
        assert m1.contains(a2);
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testSet2() {
        m1=new Membership();
        m2=new Membership();
        m1.add(a1);
        m1.add(a2);
        m1.add(a4);
        m2.add(a5);
        m2.set(m1);
        assert m2.size() == 3;
        assert m2.contains(a1);
        assert m2.contains(a2);
        assert m2.contains(a4);
        assert !m2.contains(a5);
    }


    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testMerge() {
        v1=new Vector();
        v2=new Vector();
        m1.add(a1);
        m1.add(a2);
        m1.add(a3);
        m1.add(a4);


        v1.addElement(a5);
        v2.addElement(a2);
        v2.addElement(a3);

        m1.merge(v1, v2);
        assert m1.size() == 3;
        assert m1.contains(a1);
        assert m1.contains(a4);
        assert m1.contains(a5);
    }

    @org.testng.annotations.Test(groups=Global.FUNCTIONAL)
    public void testSort() {
        m1.add(a3);
        m1.add(a4);
        m1.add(a2);
        m1.add(a1);
        m1.add(a5);
        m1.add(a2); // dupl
        System.out.println("membership: " + m1);
        Assert.assertEquals(4, m1.size());
        Assert.assertEquals(a3, m1.elementAt(0));
        Assert.assertEquals(a4, m1.elementAt(1));
        Assert.assertEquals(a1, m1.elementAt(2));
        Assert.assertEquals(a5, m1.elementAt(3));
        m1.sort();
        System.out.println("sorted: " + m1);
        Assert.assertEquals(4, m1.size());
        Assert.assertEquals(a1, m1.elementAt(0));
        Assert.assertEquals(a2, m1.elementAt(1));
        Assert.assertEquals(a4, m1.elementAt(2));
        Assert.assertEquals(a5, m1.elementAt(3));
    }



}
