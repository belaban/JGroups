// $Id: DigestTest.java,v 1.1 2007/07/04 07:29:34 belaban Exp $

package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.util.Digest;
import org.jgroups.util.MutableDigest;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.Address;

import java.io.*;
import java.util.Map;
import java.util.HashMap;


public class DigestTest extends TestCase {
    Digest         d, d2;
    MutableDigest  md;
    IpAddress      a1, a2, a3;

    public DigestTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        Map<Address, Digest.Entry> map=new HashMap<Address, Digest.Entry>();
        a1=new IpAddress(5555);
        a2=new IpAddress(6666);
        a3=new IpAddress(7777);
        map.put(a1, new Digest.Entry(4, 500, 501));
        map.put(a2, new Digest.Entry(25, 26, 26));
        map.put(a3, new Digest.Entry(20, 25, 33));
        d=new Digest(map);
        md=new MutableDigest(map);
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
    }
    
    public void testDifference(){
    	
		Map<Address, Digest.Entry> map=new HashMap<Address, Digest.Entry>();
	    a1=new IpAddress(5555);
	    a2=new IpAddress(6666);
	    a3=new IpAddress(7777);
	    map.put(a1, new Digest.Entry(4, 500, 501));
	    map.put(a2, new Digest.Entry(25, 26, 26));
	    map.put(a3, new Digest.Entry(20, 25, 33));
	    Digest digest =new Digest(map);
	     
	    Map<Address, Digest.Entry> map2=new HashMap<Address, Digest.Entry>();       
	    map2.put(a1, new Digest.Entry(4, 500, 501));
	    map2.put(a2, new Digest.Entry(25, 26, 26));
	    map2.put(a3, new Digest.Entry(20, 37, 33));
	    Digest digest2 =new Digest(map2);
	     
	    assertNotSame(digest, digest2);
	    
	    Digest diff = digest2.difference(digest);
	    System.out.println(diff);
	    assertTrue(diff.contains(a3));
        assertEquals(1, diff.size());
	    
	    
	    Map<Address, Digest.Entry> map3=new HashMap<Address, Digest.Entry>();
	    map3.put(a1, new Digest.Entry(4, 500, 501));
	    map3.put(a2, new Digest.Entry(25, 26, 26));
	    map3.put(a3, new Digest.Entry(20, 37, 33));
	    map3.put(new IpAddress(8888), new Digest.Entry(1, 2, 3));
	    Digest digest3 =new Digest(map3);
	    
	    diff = digest3.difference(digest);
	    System.out.println(diff);
        assertEquals(2, diff.size());
	    
	    diff = digest3.difference(digest2);
	    System.out.println(diff);
        assertEquals(1, diff.size());
	    
	    Digest diff2 = digest2.difference(digest3);
	    System.out.println(diff2);
        assertEquals(1, diff2.size());
        assertEquals(diff, diff2);
    }



    public void testIsGreaterThanOrEqual() {
        Map<Address, Digest.Entry> map=new HashMap<Address, Digest.Entry>();
        map.put(a1, new Digest.Entry(4, 500, 501));
        map.put(a2, new Digest.Entry(25, 26, 26));
        map.put(a3, new Digest.Entry(20, 25, 33));
        Digest my=new Digest(map);

        System.out.println("\nd: " + d + "\nmy: " + my);
        assertTrue(my.isGreaterThanOrEqual(d));

        map.remove(a3);
        map.put(a3, new Digest.Entry(20, 26, 33));
        my=new Digest(map);
        System.out.println("\nd: " + d + "\nmy: " + my);
        assertTrue(my.isGreaterThanOrEqual(d));

        map.remove(a3);
        map.put(a3, new Digest.Entry(20, 22, 32));
        my=new Digest(map);
        System.out.println("\nd: " + d + "\nmy: " + my);
        assertFalse(my.isGreaterThanOrEqual(d));
    }

    public void testEquals2() {
        md=new MutableDigest(d);
        System.out.println("d: " + d + "\nmd= " + md);
        assertEquals(d, d);
        assertEquals(d, md);
        md.incrementHighestDeliveredSeqno(a1);
        System.out.println("d: " + d + "\nmd= " + md);
        assertFalse(d.equals(md));
    }


    public void testMutability() {
        Digest md2=md;
        assertEquals(md, md2);
        md.incrementHighestDeliveredSeqno(a2);
        assertEquals(md, md2);
    }

    public void testImmutability() {
        MutableDigest tmp=new MutableDigest(d);
        assertEquals(d, tmp);
        tmp.incrementHighestDeliveredSeqno(a2);
        assertFalse(d.equals(tmp));
    }


    public void testImmutability2() {
        Digest tmp=d.copy();
        assertEquals(d, tmp);
    }

    public void testImmutability3() {
        Digest tmp=new Digest(d);
        assertEquals(tmp, d);
    }

    public void testImmutability4() {
        Digest copy=md.copy();
        assertEquals(copy, md);
        md.incrementHighestDeliveredSeqno(a1);
        assertFalse(copy.equals(md));
    }

    public void testSeal() {
        MutableDigest tmp=new MutableDigest(3);
        tmp.add(a2, 1,2,3);
        assertEquals(1, tmp.size());
        tmp.seal();
        try {
            tmp.add(a2, 4,5,6);
            fail("should run into an exception");
        }
        catch(IllegalAccessError e) {
            System.out.println("received exception \"" + e.toString() + "\" - as expected");
        }
        assertEquals(1, tmp.size());
    }


    public void testSeal2() {
        md.incrementHighestDeliveredSeqno(a1);
        md.seal();
        try {
            md.incrementHighestDeliveredSeqno(a3);
            fail("should run into an exception");
        }
        catch(IllegalAccessError e) {
            System.out.println("received exception \"" + e.toString() + "\" - as expected");
        }

        MutableDigest tmp=new MutableDigest(md);
        tmp.incrementHighestDeliveredSeqno(a3);
    }

    public void testAdd() {
        assertEquals(3, md.size());
        md.add(a1, 100, 200, 201);
        assertEquals(3, md.size());
        md.add(new IpAddress(14526), 1,2,3);
        assertEquals(4, md.size());
    }

    public void testAddDigest() {
        Digest tmp=md.copy();
        md.add(tmp);
        assertEquals(3, md.size());
    }

    public void testAddDigest2() {
        MutableDigest tmp=new MutableDigest(4);
        tmp.add(new IpAddress(1111), 1,2,3);
        tmp.add(new IpAddress(2222), 1,2,3);
        tmp.add(new IpAddress(5555), 1,2,3);
        tmp.add(new IpAddress(6666), 1,2,3);
        md.add(tmp);
        assertEquals(5, md.size());
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
        md=new MutableDigest(3);
        md.add(a1, 1, 100);
        md.add(a2, 3, 300);
        md.add(a3, 7, 700);

        long tmp=md.highestDeliveredSeqnoAt(a1);
        md.incrementHighestDeliveredSeqno(a1);
        assertEquals(md.highestDeliveredSeqnoAt(a1), tmp+1);

        tmp=md.highestDeliveredSeqnoAt(a2);
        md.incrementHighestDeliveredSeqno(a2);
        assertEquals(md.highestDeliveredSeqnoAt(a2), tmp+1);

        tmp=md.highestDeliveredSeqnoAt(a3);
        md.incrementHighestDeliveredSeqno(a3);
        assertEquals(md.highestDeliveredSeqnoAt(a3), tmp+1);
    }


    public void testConstructor() {
        assertEquals(3, md.size());
        md.clear();
        assertEquals(0, md.size());
        md.clear();
        assertEquals(0, md.size());
    }


    public void testConstructor2() {
        Digest dd=new Digest(3);
        assertEquals(0, dd.size());
    }

    public void testConstructor3() {
        Digest dd=new MutableDigest(3);
        assertEquals(0, dd.size());
    }


    public void testContains() {
        assertTrue(d.contains(a1));
        assertTrue(d.contains(a2));
        assertTrue(d.contains(a3));
    }



    public void testResetAt() {
        md.resetAt(a1);
        assertEquals(0, md.lowSeqnoAt(a1));
        assertEquals(0, md.highestDeliveredSeqnoAt(a1));
        assertEquals(0, md.highestReceivedSeqnoAt(a1));
    }


    public void testLowSeqnoAt() {
        assertEquals(4, d.lowSeqnoAt(a1));
        assertEquals(25, d.lowSeqnoAt(a2));
        assertEquals(20, d.lowSeqnoAt(a3));
    }


    public void testHighSeqnoAt() {
        assertEquals(500, d.highestDeliveredSeqnoAt(a1));
        assertEquals(26, d.highestDeliveredSeqnoAt(a2));
        assertEquals(25, d.highestDeliveredSeqnoAt(a3));
    }

//    public void testSetHighSeqnoAt() {
//        assertEquals(500, md.highSeqnoAt(a1));
//        md.setHighSeqnoAt(a1, 555);
//        assertEquals(555, md.highSeqnoAt(a1));
//    }

    public void testHighSeqnoSeenAt() {
        assertEquals(501, d.highestReceivedSeqnoAt(a1));
        assertEquals(26, d.highestReceivedSeqnoAt(a2));
        assertEquals(33, d.highestReceivedSeqnoAt(a3));
    }

//    public void testSetHighSeenSeqnoAt() {
//        assertEquals(26, md.highSeqnoSeenAt(a2));
//        md.setHighSeqnoSeenAt(a2, 100);
//        assertEquals(100, md.highSeqnoSeenAt(a2));
//    }

    public void testSetHighestDeliveredAndSeenSeqnoAt() {
        assertEquals(4, d.lowSeqnoAt(a1));
        assertEquals(500, d.highestDeliveredSeqnoAt(a1));
        assertEquals(501, md.highestReceivedSeqnoAt(a1));
        md.setHighestDeliveredAndSeenSeqnos(a1, 2, 10, 20);
        assertEquals(2, md.lowSeqnoAt(a1));
        assertEquals(10, md.highestDeliveredSeqnoAt(a1));
        assertEquals(20, md.highestReceivedSeqnoAt(a1));
    }

    public void testCopy() {
        d=d.copy();
        testLowSeqnoAt();
        testHighSeqnoAt();
        testHighSeqnoSeenAt();
        testContains();
        testResetAt();
    }


    public void testCopy2() {
        Digest tmp=d.copy();
        assertEquals(tmp, d);
    }


    public void testMutableCopy() {
        Digest copy=md.copy();
        System.out.println("md=" + md + "\ncopy=" + copy);
        assertEquals(md, copy);
        md.add(a1, 4, 500, 1000);
        System.out.println("md=" + md + "\ncopy=" + copy);
        assertFalse(md.equals(copy));
    }


    public void testMerge() {
        Map<Address, Digest.Entry> map=new HashMap<Address, Digest.Entry>();
        map.put(a1, new Digest.Entry(3, 499, 502));
        map.put(a2, new Digest.Entry(20, 26, 27));
        map.put(a3, new Digest.Entry(21, 26, 35));
        MutableDigest digest=new MutableDigest(map);

        System.out.println("d: " + d);
        System.out.println("digest: " + digest);
        
        digest.merge(d);
        System.out.println("merged digest: " + digest);

        assertEquals(3, d.size());
        assertEquals(3, digest.size());

        assertEquals(3, digest.lowSeqnoAt(a1));
        assertEquals(500, digest.highestDeliveredSeqnoAt(a1));
        assertEquals(502, digest.highestReceivedSeqnoAt(a1));

        assertEquals(20, digest.lowSeqnoAt(a2));
        assertEquals(26, digest.highestDeliveredSeqnoAt(a2));
        assertEquals(27, digest.highestReceivedSeqnoAt(a2));

        assertEquals(20, digest.lowSeqnoAt(a3));
        assertEquals(26, digest.highestDeliveredSeqnoAt(a3));
        assertEquals(35, digest.highestReceivedSeqnoAt(a3));
    }

    public void testNonConflictingMerge() {
        MutableDigest cons_d=new  MutableDigest(5);
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

        assertEquals(10, cons_d.highestDeliveredSeqnoAt(ip1));
        assertEquals(20, cons_d.highestDeliveredSeqnoAt(ip2));
        assertEquals(500, cons_d.highestDeliveredSeqnoAt(a1));
        assertEquals(26, cons_d.highestDeliveredSeqnoAt(a2));
        assertEquals(25, cons_d.highestDeliveredSeqnoAt(a3));

        assertEquals(10, cons_d.highestReceivedSeqnoAt(ip1));
        assertEquals(20, cons_d.highestReceivedSeqnoAt(ip2));
        assertEquals(501, cons_d.highestReceivedSeqnoAt(a1));
        assertEquals(26, cons_d.highestReceivedSeqnoAt(a2));
        assertEquals(33, cons_d.highestReceivedSeqnoAt(a3));
    }


    public void testConflictingMerge() {
        MutableDigest new_d=new MutableDigest(2);
        new_d.add(a1, 5, 450, 501);
        new_d.add(a3, 18, 28, 35);
        //System.out.println("\nd before: " + d);
        //System.out.println("new_: " + new_d);
        md.merge(new_d);

        assertEquals(3, md.size());
        //System.out.println("d after: " + d);

        assertEquals(4, md.lowSeqnoAt(a1));  // low_seqno should *not* have changed
        assertEquals(500, md.highestDeliveredSeqnoAt(a1));  // high_seqno should *not* have changed
        assertEquals(501, md.highestReceivedSeqnoAt(a1));  // high_seqno_seen should *not* have changed

        assertEquals(25, md.lowSeqnoAt(a2));  // low_seqno should *not* have changed
        assertEquals(26, md.highestDeliveredSeqnoAt(a2));  // high_seqno should *not* have changed
        assertEquals(26, md.highestReceivedSeqnoAt(a2));  // high_seqno_seen should *not* have changed

        assertEquals(18, md.lowSeqnoAt(a3));  // low_seqno should *not* have changed
        assertEquals(28, md.highestDeliveredSeqnoAt(a3));  // high_seqno should *not* have changed
        assertEquals(35, md.highestReceivedSeqnoAt(a3));  // high_seqno_seen should *not* have changed
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
        MutableDigest tmp=new MutableDigest(3);
        tmp.add(a1, 4, 500, 501);
        tmp.add(a3, 20, 25, 33);
        tmp.add(a2, 25, 26, 26);
        assertTrue(md.sameSenders(tmp));
        assertTrue(d.sameSenders(tmp));
    }

    public void testSameSendersNotSameLength() {
        md=new MutableDigest(3);
        md.add(a1, 4, 500, 501);
        md.add(a2, 25, 26, 26);
        assertFalse(d.sameSenders(md));
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
