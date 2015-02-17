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
    RspList rl;
    Address a1, a2, a3, a4, a5;
    Rsp rsp1, rsp2, rsp3, rsp4, rsp5;


    @BeforeMethod
    void setUp() throws Exception {
        rl=new RspList();
        a1=Util.createRandomAddress();
        a2=Util.createRandomAddress();
        a3=Util.createRandomAddress();
        a4=Util.createRandomAddress();
        a5=Util.createRandomAddress();
        rsp1=new Rsp(a1);
        rsp2=new Rsp(a2);
        rsp2.setSuspected();
        rsp3=new Rsp(a3, "hello world");
        rsp4=new Rsp(a4, Boolean.TRUE);
        rsp5=new Rsp(a5);
        rsp5.setSuspected();
        rl.put(a1, rsp1);
        rl.put(a2, rsp2);
        rl.put(a3, rsp3);
        rl.put(a4, rsp4);
        rl.put(a5, rsp5);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        rl.clear();
    }


    public void testConstructor() {
        Collection<Rsp> c=new LinkedList<>();
        c.add(rsp1); c.add(rsp2); c.add(rsp3);
        RspList tmp=new RspList(c);
        Assert.assertEquals(c.size(), tmp.size());
        assert tmp.containsKey(a1);
        assert tmp.containsKey(a2);
        assert tmp.containsKey(a3);
        assert tmp.containsValue(rsp1);
        assert tmp.containsValue(rsp2);
        assert tmp.containsValue(rsp3);
    }


    public void testIsEmpty() {
        RspList tmp=new RspList();
        assert tmp.isEmpty();
        tmp.addRsp(a1, rsp1);
        assert !(tmp.isEmpty());
    }


    public void testContainsKey() {
        assert rl.containsKey(a1);
        assert rl.containsKey(a3);
    }


    public void testContainsValue() {
        assert rl.containsValue(rsp1);
        assert rl.containsValue(rsp3);
    }


    public void testGet() {
        Rsp rsp=rl.get(a1);
        Assert.assertEquals(rsp, rsp1);
        rsp=rl.get(a3);
        Assert.assertEquals(rsp, rsp3);
    }


    public void testPut() {
        Rsp rsp, tmp;
        tmp=new Rsp(Util.createRandomAddress());
        tmp.setSuspected();
        rsp=rl.put(Util.createRandomAddress(), tmp);
        assert rsp == null;
        rsp=rl.put(a2, rsp2);
        Assert.assertEquals(rsp, rsp2);
        Assert.assertEquals(6, rl.size());
    }


    public void testRemove() {
        Rsp rsp;
        rsp=rl.remove(Util.createRandomAddress());
        assert rsp == null;
        rsp=rl.remove(a2);
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
        rl.addRsp(tmp, new Integer(322649));
        Assert.assertEquals(6, rl.size());
        Rsp rsp=rl.get(tmp);
        assert rsp != null;
        assert rsp.wasReceived();
        assert !(rsp.wasSuspected());
        Assert.assertEquals(new Integer(322649), rsp.getValue());
    }


    public void testAddRsp2() {
        rl.addRsp(a1, new Integer(322649));
        Assert.assertEquals(5, rl.size());
        Rsp rsp=rl.get(a1);
        assert rsp != null;
        assert rsp.wasReceived();
        assert !(rsp.wasSuspected());
        Assert.assertEquals(new Integer(322649), rsp.getValue());
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
        Assert.assertEquals(2, v.size());
    }


    public void testElementAt() {
        Set<Address> s=new HashSet<>();
        s.addAll(rl.keySet());
        System.out.println("-- set is " + s);
        Assert.assertEquals(rl.size(), s.size());
    }


}
