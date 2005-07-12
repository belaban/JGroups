// $Id: DigestTest.java,v 1.5 2005/07/12 10:14:50 belaban Exp $

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
        assertEquals(d2.size(), 0);
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
        assertEquals(d.size(), 3);
        d.add(a1, 100, 200, 201);
        assertEquals(d.size(), 3);
        d.add(new IpAddress(14526), 1,2,3);
        assertEquals(d.size(), 4);
    }

    public void testAddDigest() {
        d2=d.copy();
        d.add(d2);
        assertEquals(d.size(), 3);
    }

    public void testAddDigest2() {
        d2=new Digest(4);
        d2.add(new IpAddress(1111), 1,2,3);
        d2.add(new IpAddress(2222), 1,2,3);
        d2.add(new IpAddress(5555), 1,2,3);
        d2.add(new IpAddress(6666), 1,2,3);
        d.add(d2);
        assertEquals(d.size(), 5);
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
        assertTrue(d.size() == 3);
        d.clear();
        assertTrue(d.size() == 0);
        d.clear();
        assertTrue(d.size() == 0);
    }


    public void testConstructor2() {
        Digest dd=new Digest(3);
        assertEquals(dd.size(), 0);
    }


    public void testContains() {
        assertTrue(d.contains(a1));
        assertTrue(d.contains(a2));
        assertTrue(d.contains(a3));
    }



    public void testResetAt() {
        d.resetAt(a1);
        assertTrue(d.lowSeqnoAt(a1) == 0);
        assertTrue(d.highSeqnoAt(a1) == 0);
        assertTrue(d.highSeqnoSeenAt(a1) == -1);
    }


    public void testLowSeqnoAt() {
        assertTrue(d.lowSeqnoAt(a1) == 4);
        assertTrue(d.lowSeqnoAt(a2) == 25);
        assertTrue(d.lowSeqnoAt(a3) == 20);
    }


    public void testHighSeqnoAt() {
        assertTrue(d.highSeqnoAt(a1) == 500);
        assertTrue(d.highSeqnoAt(a2) == 26);
        assertTrue(d.highSeqnoAt(a3) == 25);
    }

    public void testHighSeqnoSeenAt() {
        assertTrue(d.highSeqnoSeenAt(a1) == 501);
        assertTrue(d.highSeqnoSeenAt(a2) == 26);
        assertTrue(d.highSeqnoSeenAt(a3) == 33);
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

        assertEquals(cons_d.size(), 5);
        //System.out.println("\ncons_d after: " + cons_d);
        assertTrue(cons_d.lowSeqnoAt(ip1) == 1);
        assertTrue(cons_d.lowSeqnoAt(ip2) == 2);
        assertTrue(cons_d.lowSeqnoAt(a1) == 4);
        assertTrue(cons_d.lowSeqnoAt(a2) == 25);
        assertTrue(cons_d.lowSeqnoAt(a3) == 20);

        assertTrue(cons_d.highSeqnoAt(ip1) == 10);
        assertTrue(cons_d.highSeqnoAt(ip2) == 20);
        assertTrue(cons_d.highSeqnoAt(a1) == 500);
        assertTrue(cons_d.highSeqnoAt(a2) == 26);
        assertTrue(cons_d.highSeqnoAt(a3) == 25);

        assertTrue(cons_d.highSeqnoSeenAt(ip1) == 10);
        assertTrue(cons_d.highSeqnoSeenAt(ip2) == 20);
        assertTrue(cons_d.highSeqnoSeenAt(a1) == 501);
        assertTrue(cons_d.highSeqnoSeenAt(a2) == 26);
        assertTrue(cons_d.highSeqnoSeenAt(a3) == 33);
    }


    public void testConflictingMerge() {
        Digest new_d=new Digest(2);
        new_d.add(a1, 5, 450, 501);
        new_d.add(a3, 18, 28, 35);
        //System.out.println("\nd before: " + d);
        //System.out.println("new_: " + new_d);
        d.merge(new_d);

        assertEquals(d.size(), 3);
        //System.out.println("d after: " + d);

        assertTrue(d.lowSeqnoAt(a1) == 4);  // low_seqno should *not* have changed
        assertTrue(d.highSeqnoAt(a1) == 500);  // high_seqno should *not* have changed
        assertTrue(d.highSeqnoSeenAt(a1) == 501);  // high_seqno_seen should *not* have changed

        assertTrue(d.lowSeqnoAt(a2) == 25);  // low_seqno should *not* have changed
        assertTrue(d.highSeqnoAt(a2) == 26);  // high_seqno should *not* have changed
        assertTrue(d.highSeqnoSeenAt(a2) == 26);  // high_seqno_seen should *not* have changed

        assertTrue(d.lowSeqnoAt(a3) == 18);  // low_seqno should *not* have changed
        assertTrue(d.highSeqnoAt(a3) == 28);  // high_seqno should *not* have changed
        assertTrue(d.highSeqnoSeenAt(a3) == 35);  // high_seqno_seen should *not* have changed
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
        TestSuite s=new TestSuite(DigestTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
