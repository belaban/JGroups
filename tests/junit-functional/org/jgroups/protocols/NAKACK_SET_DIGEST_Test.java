package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.util.Digest;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.TimeoutException;

/**
 * Tests setting of digest NAKACK.down(SET_DIGEST), JIRA issue is https://jira.jboss.org/jira/browse/JGRP-1060
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class NAKACK_SET_DIGEST_Test {
    private NAKACK2   nak;
    private Digest    d1, d2;
    private Address   a, b, c;
    private View      v1, v2;

    private static final short TP_ID=101;

    @BeforeMethod
    protected void setUp() throws Exception {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
        v1=View.create(a, 1, a, b);
        v2=View.create(a, 2, a, b, c);

        nak=new NAKACK2();
        d1=new Digest(v1.getMembersRaw(), new long[]{11,11, 30,35});
        d2=new Digest(v2.getMembersRaw(), new long[]{10,10, 30,30, 50,50});

        TP transport=new TP() {
            public boolean supportsMulticasting() {return false;}
            public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
            public String getInfo() {return null;}
            public Object down(Event evt) {return null;}
            public Object down(Message msg) {return null;}
            protected PhysicalAddress getPhysicalAddress() {return null;}
            public TimeScheduler getTimer() {return new TimeScheduler3();}
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
        Digest digest=(Digest)nak.down(Event.GET_DIGEST_EVT);
        System.out.println("digest = " + digest);
        assert digest.capacity() == 3;
        assert digest.containsAll(a, b, c);

        System.out.println("setting d1:");
        nak.down(new Event(Event.SET_DIGEST, d1));
        digest=(Digest)nak.down(Event.GET_DIGEST_EVT);
        System.out.println("digest = " + digest);
        assert digest.capacity() == 3; // https://jira.jboss.org/jira/browse/JGRP-1060
        assert digest.containsAll(a, b, c);
    }


}
