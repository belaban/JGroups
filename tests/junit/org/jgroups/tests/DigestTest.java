// $Id: DigestTest.java,v 1.6 2006/08/31 13:48:13 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.protocols.pbcast.Digest;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;

import java.io.*;


public class DigestTest extends TestCase {
    Digest d, d2;
    IpAddress a1, a2, a3;

    public DigestTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        d=new Digest(3);
        a1=new IpAddress(5555);
        a2=new IpAddress(6666);
        a3=new IpAddress(7777);
        d.add(a1, 4, 500, 501);
        d.add(a2, 25, 26, 26);
        d.add(a3, 20, 25, 33);
    }

    public void testSize() {
        d2=new Digest(3);
        assertEquals(0, d2.size());
    }

    public void testEquals() {
        d2=d.copy();
        System.out.println("d: " + d + "\nd2= " + d2);
        assertEquals(d, d);
        assertEquals(d, d2);
        d2.incrementHighSeqno(a1);
        assertFalse(d.equals(d2));
    }

    public void testAdd() {
        assertEquals(3, d.size());
        d.add(a1, 100, 200, 201);
        assertEquals(3, d.size());
        d.add(new IpAddress(14526), 1,2,3);
        assertEquals(4, d.size());
    }

    public void testAddDigest() {
        d2=d.copy();
        d.add(d2);
        assertEquals(3, d.size());
    }

    public void testAddDigest2() {
        d2=new Digest(4);
        d2.add(new IpAddress(1111), 1,2,3);
        d2.add(new IpAddress(2222), 1,2,3);
        d2.add(new IpAddress(5555), 1,2,3);
        d2.add(new IpAddress(6666), 1,2,3);
        d.add(d2);
        assertEquals(5, d.size());
    }

    public void testGet() {
        Digest.Entry entry;
        entry=d.get(a1);
        assertEquals(entry, new Digest.Entry(4,500,501));
        entry=d.get(a2);
        assertEquals(entry, new Digest.Entry(25,26,26));
        entry=d.get(a3);
        assertEquals(entry, new Digest.Entry(20,25,33));
    }

    public void testIncrementHighSeqno() {
        d2=new Digest(3);
        d2.add(a1, 1, 100);
        d2.add(a2, 3, 300);
        d2.add(a3, 7, 700);

        long tmp=d2.highSeqnoAt(a1);
        d2.incrementHighSeqno(a1);
        assertEquals(d2.highSeqnoAt(a1), tmp+1);

        tmp=d2.highSeqnoAt(a2);
        d2.incrementHighSeqno(a2);
        assertEquals(d2.highSeqnoAt(a2), tmp+1);

        tmp=d2.highSeqnoAt(a3);
        d2.incrementHighSeqno(a3);
        assertEquals(d2.highSeqnoAt(a3), tmp+1);
    }


    public void testConstructor() {
        assertEquals(3, d.size());
        d.clear();
        assertEquals(0, d.size());
        d.clear();
        assertEquals(0, d.size());
    }


    public void testConstructor2() {
        Digest dd=new Digest(3);
        assertEquals(0, dd.size());
    }


    public void testContains() {
        assertTrue(d.contains(a1));
        assertTrue(d.contains(a2));
        assertTrue(d.contains(a3));
    }



    public void testResetAt() {
        d.resetAt(a1);
        assertEquals(0, d.lowSeqnoAt(a1));
        assertEquals(0, d.highSeqnoAt(a1));
        assertEquals(d.highSeqnoSeenAt(a1), -1);
    }


    public void testLowSeqnoAt() {
        assertEquals(4, d.lowSeqnoAt(a1));
        assertEquals(25, d.lowSeqnoAt(a2));
        assertEquals(20, d.lowSeqnoAt(a3));
    }


    public void testHighSeqnoAt() {
        assertEquals(500, d.highSeqnoAt(a1));
        assertEquals(26, d.highSeqnoAt(a2));
        assertEquals(25, d.highSeqnoAt(a3));
    }

    public void testHighSeqnoSeenAt() {
        assertEquals(501, d.highSeqnoSeenAt(a1));
        assertEquals(26, d.highSeqnoSeenAt(a2));
        assertEquals(33, d.highSeqnoSeenAt(a3));
    }

    public void testCopy() {
        d=d.copy();
        testLowSeqnoAt();
        testHighSeqnoAt();
        testHighSeqnoSeenAt();
        testContains();
        testResetAt();
    }


    public void testNonConflictingMerge() {
        Digest cons_d=new Digest(5);
        IpAddress ip1=new IpAddress(1111), ip2=new IpAddress(2222);

        cons_d.add(ip1, 1, 10, 10);
        cons_d.add(ip2, 2, 20, 20);
        // System.out.println("\ncons_d before: " + cons_d);
        cons_d.merge(d);

        assertEquals(5, cons_d.size());
        //System.out.println("\ncons_d after: " + cons_d);
        assertEquals(1, cons_d.lowSeqnoAt(ip1));
        assertEquals(2, cons_d.lowSeqnoAt(ip2));
        assertEquals(4, cons_d.lowSeqnoAt(a1));
        assertEquals(25, cons_d.lowSeqnoAt(a2));
        assertEquals(20, cons_d.lowSeqnoAt(a3));

        assertEquals(10, cons_d.highSeqnoAt(ip1));
        assertEquals(20, cons_d.highSeqnoAt(ip2));
        assertEquals(500, cons_d.highSeqnoAt(a1));
        assertEquals(26, cons_d.highSeqnoAt(a2));
        assertEquals(25, cons_d.highSeqnoAt(a3));

        assertEquals(10, cons_d.highSeqnoSeenAt(ip1));
        assertEquals(20, cons_d.highSeqnoSeenAt(ip2));
        assertEquals(501, cons_d.highSeqnoSeenAt(a1));
        assertEquals(26, cons_d.highSeqnoSeenAt(a2));
        assertEquals(33, cons_d.highSeqnoSeenAt(a3));
    }


    public void testConflictingMerge() {
        Digest new_d=new Digest(2);
        new_d.add(a1, 5, 450, 501);
        new_d.add(a3, 18, 28, 35);
        //System.out.println("\nd before: " + d);
        //System.out.println("new_: " + new_d);
        d.merge(new_d);

        assertEquals(3, d.size());
        //System.out.println("d after: " + d);

        assertEquals(4, d.lowSeqnoAt(a1));  // low_seqno should *not* have changed
        assertEquals(500, d.highSeqnoAt(a1));  // high_seqno should *not* have changed
        assertEquals(501, d.highSeqnoSeenAt(a1));  // high_seqno_seen should *not* have changed

        assertEquals(25, d.lowSeqnoAt(a2));  // low_seqno should *not* have changed
        assertEquals(26, d.highSeqnoAt(a2));  // high_seqno should *not* have changed
        assertEquals(26, d.highSeqnoSeenAt(a2));  // high_seqno_seen should *not* have changed

        assertEquals(18, d.lowSeqnoAt(a3));  // low_seqno should *not* have changed
        assertEquals(28, d.highSeqnoAt(a3));  // high_seqno should *not* have changed
        assertEquals(35, d.highSeqnoSeenAt(a3));  // high_seqno_seen should *not* have changed
    }


    public void testSameSendersOtherIsNull() {
        assertFalse(d.sameSenders(null));
    }

    public void testSameSenders1MNullDifferentLenth() {
        d2=new Digest(1);
        assertFalse(d2.sameSenders(d));
    }

    public void testSameSenders1MNullSameLength() {
        d2=new Digest(3);
        assertFalse(d2.sameSenders(d));
    }

    public void testSameSendersIdentical() {
        d2=d.copy();
        assertTrue(d.sameSenders(d2));
    }

    public void testSameSendersNotIdentical() {
        d2=new Digest(3);
        d2.add(a1, 4, 500, 501);
        d2.add(a3, 20, 25, 33);
        d2.add(a2, 25, 26, 26);
        assertTrue(d.sameSenders(d2));
    }

    public void testSameSendersNotSameLength() {
        d2=new Digest(3);
        d2.add(a1, 4, 500, 501);
        d2.add(a2, 25, 26, 26);
        assertFalse(d.sameSenders(d2));
    }


    public void testStreamable() throws IOException, IllegalAccessException, InstantiationException {
        ByteArrayOutputStream outstream=new ByteArrayOutputStream();
        DataOutputStream dos=new DataOutputStream(outstream);
        d.writeTo(dos);
        dos.close();
        byte[] buf=outstream.toByteArray();
        ByteArrayInputStream instream=new ByteArrayInputStream(buf);
        DataInputStream dis=new DataInputStream(instream);
        Digest tmp=new Digest();
        tmp.readFrom(dis);
        assertEquals(d, tmp);
    }

    public void testSerializedSize() throws Exception {
        long len=d.serializedSize();
        byte[] buf=Util.streamableToByteBuffer(d);
        assertEquals(len, buf.length);
    }


    public static Test suite() {
        return new TestSuite(DigestTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
