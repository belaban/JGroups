package org.jgroups.protocols;

import junit.framework.TestCase;
import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.TimeoutException;
import org.jgroups.View;
import org.jgroups.util.Promise;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;

import java.util.Arrays;
import java.util.Vector;

/**
 * Tests setting of digest NAKACK.down(SET_DIGEST), JIRA issue is https://jira.jboss.org/jira/browse/JGRP-1060
 * @author Bela Ban
 * @version $Id: NAKACK_SET_DIGEST_Test.java,v 1.1.2.1 2009/09/25 08:09:19 belaban Exp $
 */
public class NAKACK_SET_DIGEST_Test extends TestCase {
    private org.jgroups.protocols.pbcast.NAKACK nak;
    private org.jgroups.protocols.pbcast.Digest d1, d2;
    private Address a, b, c;


    protected void setUp() throws Exception {
        super.setUp();
        a=new IpAddress(5000); 
        b=new IpAddress(6000);
        c=new IpAddress(7000);
        nak=new org.jgroups.protocols.pbcast.NAKACK();
        d1=new org.jgroups.protocols.pbcast.Digest(2);
        d1.add(a, 0, 10, 10);
        d1.add(b, 0, 30, 30);

        d2=new org.jgroups.protocols.pbcast.Digest(3);
        d2.add(a, 0, 10, 11);
        d2.add(b, 0, 35, 35);
        d2.add(c, 10, 50, 50);

        nak.setDownProtocol(new Protocol() {
            public String getName() {return "blo";}
            public void down(Event evt) {
            }
        });

        View view=new View(a, 1, new Vector(Arrays.asList(new Address[]{a, b, c})));
        nak.down(new Event(Event.VIEW_CHANGE, view));
    }

    public void testSetDigest() throws TimeoutException {
        MyProtocol prot=new MyProtocol();
        nak.setUpProtocol(prot);


        System.out.println("d1: " + d1);
        System.out.println("d2: " + d2);

        System.out.println("setting d2:");
        nak.down(new Event(Event.SET_DIGEST, d2));
        nak.down(new Event(Event.GET_DIGEST));
        org.jgroups.protocols.pbcast.Digest digest=(org.jgroups.protocols.pbcast.Digest)prot.promise.getResultWithTimeout(5000);
        System.out.println("digest = " + digest);
        assertEquals(3, digest.size());
        assertTrue(digest.contains(a));
        assertTrue(digest.contains(b));
        assertTrue(digest.contains(c));

        System.out.println("setting d1:");
        nak.down(new Event(Event.SET_DIGEST, d1));
        nak.down(new Event(Event.GET_DIGEST));
        digest=(org.jgroups.protocols.pbcast.Digest)prot.promise.getResultWithTimeout(5000);
        System.out.println("digest = " + digest);
        assertEquals(3, digest.size()); // https://jira.jboss.org/jira/browse/JGRP-1060
        assertTrue(digest.contains(a));
        assertTrue(digest.contains(b));
        assertTrue(digest.contains(c));
    }



    private static class MyProtocol extends Protocol {
        private Promise promise=new Promise();

        public String getName() {return "bla";}

        public void up(Event evt) {
            if(evt.getType() == Event.GET_DIGEST_OK)
                promise.setResult(evt.getArg());
        }
    }
}
