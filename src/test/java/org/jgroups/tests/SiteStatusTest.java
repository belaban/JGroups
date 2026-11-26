package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.protocols.relay.SiteStatus;
import org.jgroups.protocols.relay.SiteStatus.Status;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

/**
 * Tests {@link org.jgroups.protocols.relay.SiteStatus}
 * @author Bela Ban
 * @since  5.2.17
 */
@Test(groups= Global.FUNCTIONAL)
public class SiteStatusTest {
    public void testSiteStatus() {
        SiteStatus s=new SiteStatus();
        Set<String> res=s.add(Set.of("net1"), Status.up);
        assert res.size() == 1 && res.contains("net1");
        Status status=s.get("net1");
        assert status == Status.up;
        res=s.add(Set.of("net1"), Status.up);
        assert res.isEmpty();
        res=s.add(Set.of("net1"), Status.down);
        assert res.size() == 1 && res.contains("net1");
        status=s.get("net1");
        assert status == Status.down;

        status=s.get("hf");
        assert status == null;
    }

    public void testSiteStatus2() {
        SiteStatus s=new SiteStatus();
        Set<String> retval=s.add(Set.of("hf","net1","net2"),Status.down);
        assert retval.size() == 3;
        for(String st: Arrays.asList("hf", "net1", "net2"))
            assert s.get(st) == Status.down;
        retval=s.add(Set.of("net1"), Status.up);
        assert retval.size() == 1;
        retval=s.add(Set.of("net1", "net2"), Status.up);
        assert retval.size() == 1 && retval.contains("net2");
    }
}
