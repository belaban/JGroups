package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.TP;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Digest;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.TimeScheduler;

import java.util.Arrays;
import java.util.Vector;

/**
 * Tests setting of digest NAKACK.down(SET_DIGEST), JIRA issue is https://jira.jboss.org/jira/browse/JGRP-1060
 * @author Bela Ban
 * @version $Id: NAKACK_SET_DIGEST_Test.java,v 1.1.2.1 2009/09/25 08:40:20 belaban Exp $
 */
public class NAKACK_SET_DIGEST_Test extends TestCase {
    private NAKACK nak;
    private MutableDigest d1, d2;
    private Address a, b, c;


    protected void setUp() throws Exception {
        super.setUp();
        a=new IpAddress(5000); 
        b=new IpAddress(6000);
        c=new IpAddress(7000);
        nak=new NAKACK();
        d1=new MutableDigest(2);
        d1.add(a, 0, 10, 10);
        d1.add(b, 0, 30, 30);

        d2=new MutableDigest(3);
        d2.add(a, 0, 10, 11);
        d2.add(b, 0, 35, 35);
        d2.add(c, 10, 50, 50);

        nak.setDownProtocol(new TP() {
            public String getName() {return "blo";}
            public String getInfo() {return null;}
            public void postUnmarshalling(Message msg, Address dest, Address src, boolean multicast) {}
            public void postUnmarshallingList(Message msg, Address dest, boolean multicast) {}
            public Object down(Event evt) {return null;}
            public TimeScheduler getTimer() {return new TimeScheduler(1);}
            public void sendToAllMembers(byte[] data, int offset, int length) throws Exception {}
            public void sendToSingleMember(Address dest, byte[] data, int offset, int length) throws Exception {}
        });

        nak.setUpProtocol(new Protocol() {
            public String getName() {return "bla";}
            public Object up(Event evt) {return null;}
        });

        nak.start();
        View view=new View(a, 1, new Vector<Address>(Arrays.asList(a, b, c)));
        nak.down(new Event(Event.VIEW_CHANGE, view));
        nak.up(new Event(Event.SET_LOCAL_ADDRESS, a));
    }

    public void testSetDigest() throws TimeoutException {
        System.out.println("d1: " + d1);
        System.out.println("d2: " + d2);

        System.out.println("setting d2:");
        nak.down(new Event(Event.SET_DIGEST, d2));
        Digest digest=(Digest)nak.down(new Event(Event.GET_DIGEST));
        System.out.println("digest = " + digest);
        assertEquals(3, digest.size());
        assertTrue(digest.contains(a));
        assertTrue(digest.contains(b));
        assertTrue(digest.contains(c));

        System.out.println("setting d1:");
        nak.down(new Event(Event.SET_DIGEST, d1));
        digest=(Digest)nak.down(new Event(Event.GET_DIGEST));
        System.out.println("digest = " + digest);
        assertEquals(3, digest.size()); // https://jira.jboss.org/jira/browse/JGRP-1060
        assertTrue(digest.contains(a));
        assertTrue(digest.contains(b));
        assertTrue(digest.contains(c));
    }




}
