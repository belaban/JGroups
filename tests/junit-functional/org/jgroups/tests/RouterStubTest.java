package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.stack.RouterStub;
import org.testng.annotations.Test;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Bela Ban
 * @since  3.3
 */
@Test(groups={Global.FUNCTIONAL,Global.EAP_EXCLUDED},sequential=false)
public class RouterStubTest {

    public void testEquality() {
        RouterStub stub1=new RouterStub("192.168.1.5", 5000, null, null);
        RouterStub stub2=new RouterStub("192.168.1.5", 5000, null, null);
        assert stub1.equals(stub2);
        assert stub1.hashCode() == stub2.hashCode();
    }

    public void testInequality() {
        RouterStub stub1=new RouterStub("192.168.1.5", 5000, null, null);
        RouterStub stub2=new RouterStub("192.168.1.5", 5001, null, null);
        assert !stub1.equals(stub2);
        assert stub1.hashCode() != stub2.hashCode();
    }

    public void testCompareTo() {
        RouterStub stub1=new RouterStub("192.168.1.5", 5000, null, null);
        RouterStub stub2=new RouterStub("192.168.1.5", 5001, null, null);
        assert stub1.compareTo(stub2) == -1;
    }

    public void testHashCode() {
        RouterStub stub1=new RouterStub("192.168.1.5", 5000, null, null);
        RouterStub stub2=new RouterStub("192.168.1.5", 5001, null, null);
        RouterStub stub3=new RouterStub("192.168.1.4", 5000, null, null);
        RouterStub stub4=new RouterStub("192.168.1.5", 5000, null, null);
        Map<RouterStub,Integer> stubs=new HashMap<>();

        stubs.put(stub1, 1);
        stubs.put(stub2, 2);
        stubs.put(stub3, 3);
        System.out.println("stubs = " + stubs);
        assert stubs.size() == 3;
        stubs.put(stub4, 4);
        System.out.println("stubs = " + stubs);
        assert stubs.size() == 3;
        assert stubs.get(stub1) == 4;
        assert stubs.get(stub2) == 2;
        assert stubs.get(stub3) == 3;
    }


    public void testList() {
        Set<RouterStub> stubs=new CopyOnWriteArraySet<>();
        RouterStub stub1=new RouterStub("192.168.1.5", 5000, null, null);
        RouterStub stub2=new RouterStub("192.168.1.5", 5001, null, null);
        RouterStub stub3=new RouterStub("192.168.1.4", 5000, null, null);
        RouterStub stub4=new RouterStub("192.168.1.5", 5000, null, null);

        for(RouterStub stub: Arrays.asList(stub1, stub2, stub3, stub4))
            stubs.add(stub);
        System.out.println("stubs = " + stubs);
        assert stubs.size() == 3;

        boolean obj=stubs.remove(stub3);
        assert obj;
        assert stubs.size() == 2;
        stubs.add(stub3);
        assert stubs.size() == 3;
    }
}
