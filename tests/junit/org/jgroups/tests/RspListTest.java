package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.*;

public class RspListTest extends TestCase {
    RspList rl;
    Address a1, a2, a3, a4, a5;
    Rsp rsp1, rsp2, rsp3, rsp4, rsp5;

    public RspListTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
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

    protected void tearDown() throws Exception {
        rl.clear();
        super.tearDown();
    }

    public void testConstructor() {
        Collection c=new LinkedList();
        c.add(rsp1); c.add(rsp2); c.add(rsp3);
        RspList tmp=new RspList(c);
        assertEquals(c.size(), tmp.size());
        assertTrue(tmp.containsKey(a1));
        assertTrue(tmp.containsKey(a2));
        assertTrue(tmp.containsKey(a3));
        assertTrue(tmp.containsValue(rsp1));
        assertTrue(tmp.containsValue(rsp2));
        assertTrue(tmp.containsValue(rsp3));
    }

    public void testIsEmpty() {
        RspList tmp=new RspList();
        assertTrue(tmp.isEmpty());
        tmp.addRsp(a1, rsp1);
        assertFalse(tmp.isEmpty());
    }

    public void testContainsKey() {
        assertTrue(rl.containsKey(a1));
        assertTrue(rl.containsKey(a3));
    }

    public void testContainsValue() {
        assertTrue(rl.containsValue(rsp1));
        assertTrue(rl.containsValue(rsp3));
    }

    public void testGet() {
        Rsp rsp=(Rsp)rl.get(a1);
        assertEquals(rsp, rsp1);
        rsp=(Rsp)rl.get(a3);
        assertEquals(rsp, rsp3);
    }

    public void testPut() {
        Rsp rsp;
        rsp=(Rsp)rl.put(new IpAddress(6666), new Rsp(new IpAddress(6666), true));
        assertNull(rsp);
        rsp=(Rsp)rl.put(a2, rsp2);
        assertEquals(rsp, rsp2);
        assertEquals(6, rl.size());
    }

    public void testRemove() {
        Rsp rsp;
        rsp=(Rsp)rl.remove(new IpAddress(6666));
        assertNull(rsp);
        rsp=(Rsp)rl.remove(a2);
        assertEquals(rsp, rsp2);
        assertEquals(4, rl.size());
    }

    public void testClear() {
        rl.clear();
        assertEquals(0, rl.size());
    }

    public void testKeySet() {
        RspList tmp=new RspList();
        Set keys=tmp.keySet();
        assertNotNull(keys);
        assertEquals(0, keys.size());
    }

    public void testKeySet2() {
        Set keys=rl.keySet();
        assertNotNull(keys);
        assertEquals(rl.size(), keys.size());
    }

    public void testAddRsp() {
        rl.addRsp(new IpAddress(6666), new Integer(322649));
        assertEquals(6, rl.size());
        Rsp rsp=(Rsp)rl.get(new IpAddress(6666));
        assertNotNull(rsp);
        assertTrue(rsp.wasReceived());
        assertFalse(rsp.wasSuspected());
        assertEquals(new Integer(322649), rsp.getValue());
    }

    public void testAddRsp2() {
        rl.addRsp(a1, new Integer(322649));
        assertEquals(5, rl.size());
        Rsp rsp=(Rsp)rl.get(a1);
        assertNotNull(rsp);
        assertTrue(rsp.wasReceived());
        assertFalse(rsp.wasSuspected());
        assertEquals(new Integer(322649), rsp.getValue());
    }

    public void testNumSuspectedMembers() {
        assertEquals(2, rl.numSuspectedMembers());
    }

    public void testGetFirst() {
        Object obj=rl.getFirst();
        System.out.println("-- first (non-null) value is " + obj);
        assertNotNull(obj);
    }

    public void testGetResults() {
        Vector v=rl.getResults();
        assertNotNull(v);
        assertEquals(2, v.size());
    }

    public void testElementAt() {
        Rsp rsp;
        Set s=new HashSet();
        for(int i=0; i < rl.size(); i++) {
            rsp=(Rsp)rl.elementAt(i);
            s.add(rsp.getSender());
        }
        System.out.println("-- set is " + s);
        assertEquals(rl.size(), s.size());
    }


    public void testElementAtWithOOBEx() {
        try {
            rl.elementAt(6);
            fail("this should have thrown an ArrayIndexOutOfBoundsException");
        }
        catch(ArrayIndexOutOfBoundsException ex) {
        }
    }

    public static Test suite() {
        return new TestSuite(RspListTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(RspListTest.suite());
    }
}
