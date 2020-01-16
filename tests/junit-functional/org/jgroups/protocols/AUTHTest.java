package org.jgroups.protocols;


import org.jgroups.Global;
import org.jgroups.auth.FixedMembershipToken;
import org.jgroups.stack.IpAddress;
import org.testng.annotations.Test;

/**
 * A set of tests for the AUTH protocol
 * @author Chris Mills
 */
@Test(groups=Global.FUNCTIONAL)
public class AUTHTest {

    public void testFixedMembershipTokenIPv4() throws Exception {
        FixedMembershipToken tok=new FixedMembershipToken();
        tok.setMemberList("192.168.1.6,10.1.1.1/7500,localhost/7800");
        assert !tok.isInMembersList(new IpAddress("192.168.1.3", 7500));
        assert !tok.isInMembersList(new IpAddress("10.1.1.1", 7000));
        assert tok.isInMembersList(new IpAddress("10.1.1.1", 7500));
        assert tok.isInMembersList(new IpAddress("192.168.1.6", 7500)); // port is not matched
        assert tok.isInMembersList(new IpAddress("192.168.1.6", 0));    // port is not matched
    }


    public void testFixedMembershipTokenIPv6() throws Exception {
        FixedMembershipToken tok=new FixedMembershipToken();
        tok.setMemberList("fe80::aa20:66ff:fe11:d346,2a02:120b:2c45:1b70:aa20:66ff:fe11:d346/7500,2a02:120b:2c45:1b70:f474:e6ca:3038:6b5f/7500");
        assert tok.isInMembersList(new IpAddress("2a02:120b:2c45:1b70:f474:e6ca:3038:6b5f", 7500));
    }
}
