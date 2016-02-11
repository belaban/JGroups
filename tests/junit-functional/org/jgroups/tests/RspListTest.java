package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class RspListTest {
    RspList<Object> rl;
    Address a, b, c, d, e;
    Rsp rsp1, rsp2, rsp3, rsp4, rsp5;


    @BeforeMethod
    void setUp() throws Exception {
        rl=new RspList<>();
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
        d=Util.createRandomAddress("D");
        e=Util.createRandomAddress("E");
        rsp1=new Rsp(a);
        rsp2=new Rsp(b);
        rsp2.setSuspected();
        rsp3=new Rsp("hello world");
        rsp4=new Rsp(Boolean.TRUE);
        rsp5=new Rsp(e);
        rsp5.setSuspected();
        rl.put(a, rsp1);
        rl.put(b, rsp2);
        rl.put(c, rsp3);
        rl.put(d, rsp4);
        rl.put(e, rsp5);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        rl.clear();
    }


    public void testConstructor() {
        RspList tmp=new RspList()
          .addRsp(a, a).addRsp(b, b).addRsp(c, "hello world");
        System.out.println("tmp = " + tmp);
        Assert.assertEquals(3, tmp.size());
        assert tmp.containsKey(a);
        assert tmp.containsKey(b);
        assert tmp.containsKey(c);
        assert tmp.containsValue(rsp1);
        assert tmp.containsValue(rsp2);
        assert tmp.containsValue(rsp3);
    }


    public void testIsEmpty() {
        RspList tmp=new RspList();
        assert tmp.isEmpty();
        tmp.addRsp(a, rsp1);
        assert !(tmp.isEmpty());
    }


    public void testContainsKey() {
        assert rl.containsKey(a);
        assert rl.containsKey(c);
    }


    public void testContainsValue() {
        assert rl.containsValue(rsp1);
        assert rl.containsValue(rsp3);
    }


    public void testGet() {
        Rsp rsp=rl.get(a);
        Assert.assertEquals(rsp, rsp1);
        rsp=rl.get(c);
        Assert.assertEquals(rsp, rsp3);
    }


    public void testPut() {
        Rsp rsp, tmp;
        tmp=new Rsp(Util.createRandomAddress());
        tmp.setSuspected();
        rsp=rl.put(Util.createRandomAddress(), tmp);
        assert rsp == null;
        rsp=rl.put(b, rsp2);
        Assert.assertEquals(rsp, rsp2);
        Assert.assertEquals(6, rl.size());
    }


    public void testRemove() {
        Rsp rsp;
        rsp=rl.remove(Util.createRandomAddress());
        assert rsp == null;
        rsp=rl.remove(b);
        Assert.assertEquals(rsp, rsp2);
        Assert.assertEquals(4, rl.size());
    }


    public void testClear() {
        rl.clear();
        Assert.assertEquals(0, rl.size());
    }


    public static void testKeySet() {
        RspList tmp=new RspList();
        Set keys=tmp.keySet();
        assert keys != null;
        Assert.assertEquals(0, keys.size());
    }


    public void testKeySet2() {
        Set keys=rl.keySet();
        assert keys != null;
        Assert.assertEquals(rl.size(), keys.size());
    }


    public void testAddRsp() {
        Address tmp=Util.createRandomAddress();
        rl.addRsp(tmp, 322649);
        Assert.assertEquals(6, rl.size());
        Rsp rsp=rl.get(tmp);
        assert rsp != null;
        assert rsp.wasReceived();
        assert !(rsp.wasSuspected());
        Assert.assertEquals(322649, rsp.getValue());
    }


    public void testAddRsp2() {
        rl.addRsp(a, 322649);
        Assert.assertEquals(5, rl.size());
        Rsp rsp=rl.get(a);
        assert rsp != null;
        assert rsp.wasReceived();
        assert !(rsp.wasSuspected());
        Assert.assertEquals(322649, rsp.getValue());
    }


    public void testNumSuspectedMembers() {
        Assert.assertEquals(2, rl.numSuspectedMembers());
    }


    public void testGetFirst() {
        Object obj=rl.getFirst();
        System.out.println("-- first (non-null) value is " + obj);
        assert obj != null;
    }


    public void testGetResults() {
        List v=rl.getResults();
        assert v != null;
        Assert.assertEquals(5, v.size());
    }


    public void testElementAt() {
        Set<Address> s=new HashSet<>();
        s.addAll(rl.keySet());
        System.out.println("-- set is " + s);
        Assert.assertEquals(rl.size(), s.size());
    }


}
