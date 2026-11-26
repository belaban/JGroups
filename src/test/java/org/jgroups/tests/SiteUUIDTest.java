package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.relay.SiteMaster;
import org.jgroups.protocols.relay.SiteUUID;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests {@link org.jgroups.protocols.relay.SiteUUID}
 * @author Bela Ban
 * @since  5.2.15
 */
@Test(groups= Global.FUNCTIONAL)
public class SiteUUIDTest {
    protected static final UUID A=(UUID)Util.createRandomAddress(), B=(UUID)Util.createRandomAddress();

    public void testCompareAndHashcode() {
        SiteUUID u1=new SiteUUID(A, "A", "sfo"), u2=u1;
        Set<Address> s=new HashSet<>();
        assert u1.equals(u2);
        assert u2.equals(u1);
        s.add(u1);
        s.add(u2);
        assert s.size() == 1;

        u2=new SiteUUID(A, "A2", "sfo");
        assert u1.equals(u2); // despite the different name: we compare sites, then UUIDs!
        s.clear();
        s.add(u1); s.add(u2);
        assert s.size() == 1;

        u2=new SiteUUID(B, "B", "sfo");
        assert !u1.equals(u2);
        s.clear();
        s.add(u1); s.add(u2);
        assert s.size() == 2;
    }

    public void testCopy() {
        SiteUUID u1=new SiteUUID(A, "A", "sfo"), u2=(SiteUUID)u1.copy();
        assert u1.equals(u2);
    }

    public void testSiteMaster() {
        SiteMaster sm1=new SiteMaster("sfo"), sm2=new SiteMaster("sfo");
        assert sm1.equals(sm2);
        Set<Address> s=new HashSet<>();
        s.add(sm1); s.add(sm2);
        assert s.size() == 1;

        sm2=new SiteMaster("nyc");
        assert !sm1.equals(sm2);
        s.clear();
        s.add(sm1); s.add(sm2);
        assert s.size() == 2;

        sm2=new SiteMaster(null); // to all sites
        assert sm1.equals(sm1);
        assert sm2.equals(sm2);
        assert !sm1.equals(sm2);
        assert !sm2.equals(sm1);
        s.clear();
        s.add(sm1); s.add(sm2);
        assert s.size() == 2;

        sm1=new SiteMaster(null);
        assert sm1.equals(sm2);
        assert sm2.equals(sm1);
        s.clear();
        s.add(sm1); s.add(sm2);
        assert s.size() == 1;
    }

    public void testComparison() {
        SiteUUID sm=new SiteMaster("sfo"),
          a=new SiteUUID((UUID)Util.createRandomAddress("A"), "A", "sfo");
        assert !sm.equals(a);
        assert !a.equals(sm);
        Set<Address> s=new HashSet<>();
        s.add(sm); s.add(a);
        assert s.size() == 2;
        s.add(new SiteMaster(null));
        assert s.size() == 3;
        s.add(new SiteMaster(null));
        assert s.size() == 3;
    }
}
