package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Digest;
import org.jgroups.util.TimeScheduler;
import org.jgroups.util.TimeScheduler3;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests setting of digest NAKACK.down(SET_DIGEST), JIRA issue is https://issues.redhat.com/browse/JGRP-1060
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="create")
public class NAKACK_SET_DIGEST_Test {
    private Protocol  nak;
    private Digest    d1, d2;
    private Address   a, b, c;
    private View      v1, v2;

    private static final short TP_ID=101;

    @DataProvider
    static Object[][] create() {
        return new Object[][]{
          {new NAKACK2()},
          {new NAKACK4()}
        };
    }

    protected void setUp(Protocol prot) throws Exception {
        a=Util.createRandomAddress("A");
        b=Util.createRandomAddress("B");
        c=Util.createRandomAddress("C");
        v1=View.create(a, 1, a, b);
        v2=View.create(a, 2, a, b, c);

        this.nak=prot;
        d1=new Digest(v1.getMembersRaw(), new long[]{11,11, 30,35});
        d2=new Digest(v2.getMembersRaw(), new long[]{10,10, 30,30, 50,50});

        final TP transport=getTransport();
        nak.setDownProtocol(transport);
        nak.start();
    }

    private static TP getTransport() {
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
        return transport;
    }

    @AfterMethod
    protected void tearDown() {
        nak.stop();
    }

    public void testSetDigest(Protocol prot) throws Exception {
        setUp(prot);
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
        assert digest.capacity() == 3; // https://issues.redhat.com/browse/JGRP-1060
        assert digest.containsAll(a, b, c);
    }


}
