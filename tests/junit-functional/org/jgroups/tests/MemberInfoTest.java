package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.protocols.relay.Topology.MemberInfo;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * Tests {@link MemberInfo}
 * @author Bela Ban
 * @since  5.2.15
 */
@Test(groups= Global.FUNCTIONAL)
public class MemberInfoTest {
    protected final Address a=Util.createRandomAddress("A"), b=Util.createRandomAddress("B");

    public void testEquals() {
        MemberInfo m1=new MemberInfo("sfo", a, null, false),
          m2=new MemberInfo("sfo", b, null, true);
        assert !m1.equals(m2);
        m2=m1;
        assert m1.equals(m2);
        m2=new MemberInfo("nyc", a, null, true);
        assert m2.equals(m1);
    }

    public void testHashcode() {
        Set<MemberInfo> set=new HashSet<>();
        MemberInfo m1=new MemberInfo("sfo", a, null, false),
          m2=new MemberInfo("sfo", b, null, true);
        set.add(m1);
        set.add(m2);
        assert set.size() == 2;
        set.clear();
        set.add(m1);
        m2=m1;
        set.add(m2);
        assert set.size() == 1;
        m2=new MemberInfo("nyc", a, null, true);
        set.add(m2);
        assert set.size() == 1;
    }
}
