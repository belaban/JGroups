package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

@Test(groups=Global.FUNCTIONAL,sequential=true)
public class RspListTest {
    RspList rl;
    Address a1, a2, a3, a4, a5;
    Rsp rsp1, rsp2, rsp3, rsp4, rsp5;


    @BeforeMethod
    public void setUp() throws Exception {
        rl=new RspList();
        a1=new IpAddress(1111);
        a2=new IpAddress(2222);
        a3=new IpAddress(3333);
        a4=new IpAddress(4444);
        a5=new IpAddress(5555);
        rsp1=new Rsp(a1);
        rsp2=new Rsp(a2, true);
        rsp3=new Rsp(a3, "hello world");
        rsp4=new Rsp(a4, Boolean.TRUE);
        rsp5=new Rsp(a5, true);
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

    @Test(groups=Global.FUNCTIONAL)
    public void testConstructor() {
        Collection c=new LinkedList();
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

    @Test(groups=Global.FUNCTIONAL)
    public void testIsEmpty() {
        RspList tmp=new RspList();
        assert tmp.isEmpty();
        tmp.addRsp(a1, rsp1);
        assert !(tmp.isEmpty());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testContainsKey() {
        assert rl.containsKey(a1);
        assert rl.containsKey(a3);
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testContainsValue() {
        assert rl.containsValue(rsp1);
        assert rl.containsValue(rsp3);
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testGet() {
        Rsp rsp=(Rsp)rl.get(a1);
        Assert.assertEquals(rsp, rsp1);
        rsp=(Rsp)rl.get(a3);
        Assert.assertEquals(rsp, rsp3);
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testPut() {
        Rsp rsp;
        rsp=(Rsp)rl.put(new IpAddress(6666), new Rsp(new IpAddress(6666), true));
        assert rsp == null;
        rsp=(Rsp)rl.put(a2, rsp2);
        Assert.assertEquals(rsp, rsp2);
        Assert.assertEquals(6, rl.size());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testRemove() {
        Rsp rsp;
        rsp=(Rsp)rl.remove(new IpAddress(6666));
        assert rsp == null;
        rsp=(Rsp)rl.remove(a2);
        Assert.assertEquals(rsp, rsp2);
        Assert.assertEquals(4, rl.size());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testClear() {
        rl.clear();
        Assert.assertEquals(0, rl.size());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testKeySet() {
        RspList tmp=new RspList();
        Set keys=tmp.keySet();
        assert keys != null;
        Assert.assertEquals(0, keys.size());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testKeySet2() {
        Set keys=rl.keySet();
        assert keys != null;
        Assert.assertEquals(rl.size(), keys.size());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testAddRsp() {
        rl.addRsp(new IpAddress(6666), new Integer(322649));
        Assert.assertEquals(6, rl.size());
        Rsp rsp=(Rsp)rl.get(new IpAddress(6666));
        assert rsp != null;
        assert rsp.wasReceived();
        assert !(rsp.wasSuspected());
        Assert.assertEquals(new Integer(322649), rsp.getValue());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testAddRsp2() {
        rl.addRsp(a1, new Integer(322649));
        Assert.assertEquals(5, rl.size());
        Rsp rsp=(Rsp)rl.get(a1);
        assert rsp != null;
        assert rsp.wasReceived();
        assert !(rsp.wasSuspected());
        Assert.assertEquals(new Integer(322649), rsp.getValue());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testNumSuspectedMembers() {
        Assert.assertEquals(2, rl.numSuspectedMembers());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testGetFirst() {
        Object obj=rl.getFirst();
        System.out.println("-- first (non-null) value is " + obj);
        assert obj != null;
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testGetResults() {
        Vector v=rl.getResults();
        assert v != null;
        Assert.assertEquals(2, v.size());
    }

    @Test(groups=Global.FUNCTIONAL)
    public void testElementAt() {
        Rsp rsp;
        Set s=new HashSet();
        for(int i=0; i < rl.size(); i++) {
            rsp=(Rsp)rl.elementAt(i);
            s.add(rsp.getSender());
        }
        System.out.println("-- set is " + s);
        Assert.assertEquals(rl.size(), s.size());
    }


    @Test(groups=Global.FUNCTIONAL)
    public void testElementAtWithOOBEx() {
        try {
            rl.elementAt(6);
            assert false : "this should have thrown an ArrayIndexOutOfBoundsException";
        }
        catch(ArrayIndexOutOfBoundsException ex) {
        }
    }

}
