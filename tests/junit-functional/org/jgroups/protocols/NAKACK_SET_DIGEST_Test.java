package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.util.*;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests setting of digest NAKACK.down(SET_DIGEST), JIRA issue is https://jira.jboss.org/jira/browse/JGRP-1060
 * @author Bela Ban
 * @version $Id: NAKACK_SET_DIGEST_Test.java,v 1.1 2009/09/25 08:28:34 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL)
public class NAKACK_SET_DIGEST_Test {
    private NAKACK nak;
    private MutableDigest d1, d2;
    private Address a, b, c;

    @BeforeMethod
    protected void setUp() throws Exception {
        a=Util.createRandomAddress();
        b=Util.createRandomAddress();
        c=Util.createRandomAddress();
        UUID.add((UUID)a, "A"); UUID.add((UUID)b, "B"); UUID.add((UUID)c, "C");
        nak=new NAKACK();
        d1=new MutableDigest(2);
        d1.add(a, 0, 11, 11);
        d1.add(b, 0, 30, 35);

        d2=new MutableDigest(3);
        d2.add(a, 0, 10, 10);
        d2.add(b, 0, 30, 30);
        d2.add(c, 10, 50, 50);

        nak.setDownProtocol(new TP() {
            public String getName() {return "blo";}
            public void sendMulticast(byte[] data, int offset, int length) throws Exception {}
            public void sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
            public String getInfo() {return null;}
            public Object down(Event evt) {return null;}
            protected PhysicalAddress getPhysicalAddress() {return null;}
            public TimeScheduler getTimer() {return new TimeScheduler(1);}
        });

        nak.start();
//        View view=new View(a, 1, new Vector(Arrays.asList(new Address[]{a, b, c})));
//        nak.down(new Event(Event.VIEW_CHANGE, view));
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
