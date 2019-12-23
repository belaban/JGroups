package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.NameCache;
import org.jgroups.util.Owner;
import org.jgroups.util.UUID;
import org.testng.annotations.Test;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Bela Ban
 * @since  4.1.9
 */
@Test(groups=Global.FUNCTIONAL)
public class OwnerTest {
    protected final Address a=create(1, "A"), b=create(2, "B");

    public void testEquals() {
        Owner o1=new Owner(a, 1), o2;

        o2=o1;
        assert o1.equals(o2);

        o2=null;
        assert !o1.equals(o2);

        o2=new Owner(null, 1);
        assert !o1.equals(o2);

        o2=new Owner(a, 1);
        assert o1.equals(o2);

        o2=new Owner(a, 2);
        assert !o1.equals(o2);

        o2=new Owner(b, 1);
        assert !o1.equals(o2);
    }

    public void testCompareTo() {
        Set<Owner> s=new ConcurrentSkipListSet<>();

        Owner o1=new Owner(a, 1), o2;
        s.add(o1);

        int rc;
        o2=o1;
        s.add(o2);
        rc=o1.compareTo(o2);
        assert rc == 0;
        assert s.size() == 1;

        o2=null;
        rc=o1.compareTo(o2);
        assert rc > 0;

        o2=new Owner(null, 1);
        rc=o1.compareTo(o2);
        assert rc > 0;

        o2=new Owner(null, 2);
        rc=o1.compareTo(o2);
        assert rc < 0;

        o2=new Owner(a, 1);
        rc=o1.compareTo(o2);
        assert rc == 0;

        o2=new Owner(a, 2);
        rc=o1.compareTo(o2);
        assert rc < 0;

        o2=new Owner(b, 1);
        rc=o1.compareTo(o2);
        assert  rc < 0;
    }

    protected static Address create(int bits, String name) {
        UUID uuid=new UUID(bits, 0);
        NameCache.add(uuid, name);
        return uuid;
    }
}
