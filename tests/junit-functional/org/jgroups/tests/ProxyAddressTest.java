package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.ProxyAddress;
import org.jgroups.util.Util;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class ProxyAddressTest {
    protected Address local;
    protected Address proxy_addr1;
    protected Address proxy_addr2;

    @BeforeClass
    protected void init() {
        local=Util.createRandomAddress("A"); // the local address which acts as proxy
        proxy_addr1=new ProxyAddress(local, Util.createRandomAddress("X"));
        proxy_addr2=new ProxyAddress(local, Util.createRandomAddress("Y"));
    }

    public static void testSorting() {
        Set<Address> set=new TreeSet<Address>();
        Address id1=Util.createRandomAddress("A"),
          id2=Util.createRandomAddress("B"),
          id3=Util.createRandomAddress("X");
        ProxyAddress proxy=new ProxyAddress(id2, id3);

        set.add(id2);
        set.add(id2);
        set.add(proxy);
        System.out.println("set = " + set);

        set.clear();
        set.add(proxy);
        set.add(id1); // this fails
        set.add(id2);
        System.out.println("set = " + set);
    }


    public void testEquals() {
        Set<Address> set=new HashSet<Address>();
        set.add(local);
        set.add(proxy_addr1);
        set.add(proxy_addr2);
        System.out.println("set = " + set);
        assert set.size() == 3 : "set: " + set;

        // now add in different order
        set.clear();
        set.add(proxy_addr1);
        set.add(proxy_addr2);
        set.add(local);
        System.out.println("set = " + set);
        assert set.size() == 3 : "set: " + set;
    }


    public void testEqualsInReverseOrder() {
        Set<Address> set=new HashSet<Address>();
        set.add(proxy_addr1);
        set.add(proxy_addr2);
        set.add(local);
        System.out.println("set = " + set);
        assert set.size() == 3 : "set: " + set;
    }


    public void test2() {
        List<Address> old_list=new LinkedList<Address>(),
          new_list=new LinkedList<Address>();
        old_list.add(local);
        old_list.add(proxy_addr1);

        new_list.add(local);
        new_list.add(proxy_addr1);
        new_list.add(proxy_addr2);

        Address joiner=getMemberJoined(old_list, new_list);
        System.out.println("joiner = " + joiner);
        assert joiner != null;
        assert joiner.equals(proxy_addr2);
    }

    public void testWithHashMap() {
        Map<Address,String> map=new HashMap<Address,String>(5);
        map.put(local, "local");
        map.put(proxy_addr1, "proxy1");
        map.put(proxy_addr2, "proxy2");
        System.out.println("map = " + map);
        assert map.size() == 3;
    }



    private static Address getMemberJoined(List<Address> oldList, List<Address> newList) {
        Set<Address> tmp = new HashSet<Address>(newList);
        tmp.removeAll(oldList);
        return tmp.isEmpty() ? null : tmp.iterator().next();
    }


}
