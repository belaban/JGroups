package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests setting of digest NAKACK.down(SET_DIGEST), JIRA issue is https://jira.jboss.org/jira/browse/JGRP-1060
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class NAKACK_SET_DIGEST_Test {
    private NAKACK2 nak;
    private MutableDigest d1, d2;
    private Address a, b, c;

    private static final short TP_ID=101;

    @BeforeMethod
    protected void setUp() throws Exception {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
        nak=new NAKACK2();
        d1=new MutableDigest(2);
        d1.add(a, 11, 11);
        d1.add(b, 30, 35);

        d2=new MutableDigest(3);
        d2.add(a, 10, 10);
        d2.add(b, 30, 30);
        d2.add(c, 50, 50);

        TP transport=new TP() {
            public boolean supportsMulticasting() {return false;}
            public void sendMulticast(byte[] data, int offset, int length) throws Exception {}
            public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
            public String getInfo() {return null;}
            public Object down(Event evt) {return null;}
            protected PhysicalAddress getPhysicalAddress() {return null;}
            public TimeScheduler getTimer() {return new DefaultTimeScheduler(1);}
        };
        transport.setId(TP_ID);

        nak.setDownProtocol(transport);

        nak.start();
    }

    @AfterMethod
    protected void tearDown() {
        nak.stop();
    }

    public void testSetDigest() throws TimeoutException {
        System.out.println("d1: " + d1);
        System.out.println("d2: " + d2);

        System.out.println("setting d2:");
        nak.down(new Event(Event.SET_DIGEST, d2));
        Digest digest=(Digest)nak.down(new Event(Event.GET_DIGEST));
        System.out.println("digest = " + digest);
        assert digest.size() == 3;
        assert digest.contains(a);
        assert digest.contains(b);
        assert digest.contains(c);

        System.out.println("setting d1:");
        nak.down(new Event(Event.SET_DIGEST, d1));
        digest=(Digest)nak.down(new Event(Event.GET_DIGEST));
        System.out.println("digest = " + digest);
        assert digest.size() == 3; // https://jira.jboss.org/jira/browse/JGRP-1060
        assert digest.contains(a);
        assert digest.contains(b);
        assert digest.contains(c);
    }


}
