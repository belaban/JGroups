package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.ProxyAddress;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.TreeSet;

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

}
