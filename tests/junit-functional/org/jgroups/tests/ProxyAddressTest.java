package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.ProxyAddress;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.*;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class ProxyAddressTest {

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


    public static void testEquals() {
        Address proxy=Util.createRandomAddress("A"); // the local address which acts as proxy
        Address proxy_addr1=new ProxyAddress(proxy, Util.createRandomAddress("X"));
        Address proxy_addr2=new ProxyAddress(proxy, Util.createRandomAddress("Y"));

        Set<Address> set=new HashSet<Address>();
        set.add(proxy);
        set.add(proxy_addr1);
        set.add(proxy_addr2);
        System.out.println("set = " + set);
        assert set.size() == 3 : "set: " + set;

        // now add in different order
        set.clear();
        set.add(proxy_addr1);
        set.add(proxy_addr2);
        set.add(proxy);
            System.out.println("set = " + set);
                    assert set.size() == 3 : "set: " + set;

    }


    public static void testEqualsInReverseOrder() {
        Address proxy=Util.createRandomAddress("A"); // the local address which acts as proxy
        Address proxy_addr1=new ProxyAddress(proxy, Util.createRandomAddress("X"));
        Address proxy_addr2=new ProxyAddress(proxy, Util.createRandomAddress("Y"));

        Set<Address> set=new HashSet<Address>();
        set.add(proxy_addr1);
        set.add(proxy_addr2);
        set.add(proxy);
        System.out.println("set = " + set);
        assert set.size() == 3 : "set: " + set;
    }


    public static void test2() {
        Address local=Util.createRandomAddress("A");
        Address remote_1=new ProxyAddress(local, Util.createRandomAddress("X"));
        Address remote_2=new ProxyAddress(local, Util.createRandomAddress("Y"));

        List<Address> old_list=new LinkedList<Address>(),
          new_list=new LinkedList<Address>();
        old_list.add(local);
        old_list.add(remote_1);

        new_list.add(local);
        new_list.add(remote_1);
        new_list.add(remote_2);

        Address joiner=getMemberJoined(old_list, new_list);
        System.out.println("joiner = " + joiner);
        assert joiner != null;
        assert joiner.equals(remote_2);
    }



    private static Address getMemberJoined(List<Address> oldList, List<Address> newList) {
        Set<Address> tmp = new HashSet<Address>(newList);
        tmp.removeAll(oldList);
        return tmp.isEmpty() ? null : tmp.iterator().next();
    }


}
