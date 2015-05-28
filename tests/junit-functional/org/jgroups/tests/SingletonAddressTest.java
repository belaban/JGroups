package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.util.*;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

import java.util.*;

/**
 * Tests {@link SingletonAddress}
 * @author Bela Ban
 * @since  3.5
 */
@Test(groups=Global.FUNCTIONAL)
public class SingletonAddressTest {
    protected static final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");

    public static void testCompareTo() {
        SingletonAddress sa=new SingletonAddress("cluster".getBytes(), a);
        SingletonAddress sb=new SingletonAddress("cluster".getBytes(), a);
        assert sa.equals(sb);
        assert sa.compareTo(sb) == 0;

        sb=new SingletonAddress("cluster".getBytes(), b);
        assert !sa.equals(sb);
        assert sa.compareTo(sb) != 0;

        sb=new SingletonAddress("cluster2".getBytes(), a);
        assert !sa.equals(sb);
        assert sa.compareTo(sb) != 0;
    }

    public static void testCompareTo2() {
        SingletonAddress sa=new SingletonAddress("cluster".getBytes(), a);
        SingletonAddress sb=new SingletonAddress("cluster".getBytes(), b);
        SingletonAddress sc=new SingletonAddress("cluster".getBytes(), a);
        Map<Address,Integer> map=new HashMap<>(3);
        map.put(sa, 1);
        map.put(sb,2);
        map.put(sc,3);
        System.out.println("map = " + map);
        assert map.size() == 2;
        assert map.keySet().contains(sa);
        assert map.keySet().contains(sc);
    }

    @Test
    public void testBundlingWithSiteAddress() {
        final Map<SingletonAddress, List<Object>> msgs = new HashMap<>(24);
        final byte[] cname = new AsciiString("cluster").chars();
        final Object randomObject = new Object();

        for (Address address : Arrays.asList(
                new UUID(1, 0),
                new ExtendedUUID(1, 0),
                new SiteUUID(1, 0, "name", "site1"),
                new SiteUUID(1, 0, null, "site1"),
                new SiteUUID(1, 0, "name", "site2"),
                new SiteUUID(1, 0, null, "site2")
        )) {
            SingletonAddress dest = new SingletonAddress(cname, address);
            List<Object> tmp = msgs.get(dest);
            if (tmp == null) {
                tmp = new LinkedList<>();
                msgs.put(dest, tmp);
            }
            tmp.add(randomObject);
        }

        assert msgs.size() == 3;
        assert msgs.get(new SingletonAddress(cname, new UUID(1, 0))).size() == 2;
        assert msgs.get(new SingletonAddress(cname, new SiteUUID(1, 0, null, "site1"))).size() == 2;
        assert msgs.get(new SingletonAddress(cname, new SiteUUID(1, 0, null, "site2"))).size() == 2;
    }

}
