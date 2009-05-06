
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.util.Digest;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bela Ban
 * @version $Id: DigestTest.java,v 1.5 2009/05/06 06:05:12 belaban Exp $
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class DigestTest {
    Digest         d, d2;
    MutableDigest  md;
    Address        a1, a2, a3;


    @BeforeClass
    void beforeClass() throws Exception {
        a1=Util.createRandomAddress();
        a2=Util.createRandomAddress();
        a3=Util.createRandomAddress();
    }

    @BeforeMethod
    void beforeMethod() {
        Map<Address, Digest.Entry> map=new HashMap<Address, Digest.Entry>();
        map.put(a1, new Digest.Entry(4, 500, 501));
        map.put(a2, new Digest.Entry(25, 26, 26));
        map.put(a3, new Digest.Entry(20, 25, 33));
        d=new Digest(map);
        md=new MutableDigest(map);
    }


    public void testSize() {
        d2=new Digest(3);
        Assert.assertEquals(0, d2.size());
    }


    public void testEquals() {
        d2=d.copy();
        System.out.println("d: " + d + "\nd2= " + d2);
        Assert.assertEquals(d, d);
        Assert.assertEquals(d, d2);
    }
    

    public void testDifference(){
		Map<Address, Digest.Entry> map=new HashMap<Address, Digest.Entry>();
	    map.put(a1, new Digest.Entry(4, 500, 501));
	    map.put(a2, new Digest.Entry(25, 26, 26));
	    map.put(a3, new Digest.Entry(20, 25, 33));
	    Digest digest =new Digest(map);
	     
	    Map<Address, Digest.Entry> map2=new HashMap<Address, Digest.Entry>();       
	    map2.put(a1, new Digest.Entry(4, 500, 501));
	    map2.put(a2, new Digest.Entry(25, 26, 26));
	    map2.put(a3, new Digest.Entry(20, 37, 33));
	    Digest digest2 =new Digest(map2);

        Assert.assertNotSame(digest, digest2);
	    
	    Digest diff = digest2.difference(digest);
	    System.out.println(diff);
        assert diff.contains(a3);
        Assert.assertEquals(1, diff.size());
	    
	    
	    Map<Address, Digest.Entry> map3=new HashMap<Address, Digest.Entry>();
	    map3.put(a1, new Digest.Entry(4, 500, 501));
	    map3.put(a2, new Digest.Entry(25, 26, 26));
	    map3.put(a3, new Digest.Entry(20, 37, 33));
	    map3.put(Util.createRandomAddress(), new Digest.Entry(1, 2, 3));
	    Digest digest3 =new Digest(map3);
	    
	    diff = digest3.difference(digest);
	    System.out.println(diff);
        Assert.assertEquals(2, diff.size());
	    
	    diff = digest3.difference(digest2);
	    System.out.println(diff);
        Assert.assertEquals(1, diff.size());
	    
	    Digest diff2 = digest2.difference(digest3);
	    System.out.println(diff2);
        Assert.assertEquals(1, diff2.size());
        Assert.assertEquals(diff, diff2);
    }




    public void testIsGreaterThanOrEqual() {
        Map<Address, Digest.Entry> map=new HashMap<Address, Digest.Entry>();
        map.put(a1, new Digest.Entry(4, 500, 501));
        map.put(a2, new Digest.Entry(25, 26, 26));
        map.put(a3, new Digest.Entry(20, 25, 33));
        Digest my=new Digest(map);

        System.out.println("\nd: " + d + "\nmy: " + my);
        assert my.isGreaterThanOrEqual(d);

        map.remove(a3);
        map.put(a3, new Digest.Entry(20, 26, 33));
        my=new Digest(map);
        System.out.println("\nd: " + d + "\nmy: " + my);
        assert my.isGreaterThanOrEqual(d);

        map.remove(a3);
        map.put(a3, new Digest.Entry(20, 22, 32));
        my=new Digest(map);
        System.out.println("\nd: " + d + "\nmy: " + my);
        assert !(my.isGreaterThanOrEqual(d));
    }


    public void testEquals2() {
        md=new MutableDigest(d);
        System.out.println("d: " + d + "\nmd= " + md);
        Assert.assertEquals(d, d);
        Assert.assertEquals(d, md);
        md.incrementHighestDeliveredSeqno(a1);
        System.out.println("d: " + d + "\nmd= " + md);
        assert !(d.equals(md));
    }


    public void testMutability() {
        Digest md2=md;
        Assert.assertEquals(md, md2);
        md.incrementHighestDeliveredSeqno(a2);
        Assert.assertEquals(md, md2);
    }


    public void testImmutability() {
        MutableDigest tmp=new MutableDigest(d);
        Assert.assertEquals(d, tmp);
        tmp.incrementHighestDeliveredSeqno(a2);
        assert !(d.equals(tmp));
    }



    public void testImmutability2() {
        Digest tmp=d.copy();
        Assert.assertEquals(d, tmp);
    }


    public void testImmutability3() {
        Digest tmp=new Digest(d);
        Assert.assertEquals(tmp, d);
    }


    public void testImmutability4() {
        Digest copy=md.copy();
        Assert.assertEquals(copy, md);
        md.incrementHighestDeliveredSeqno(a1);
        assert !(copy.equals(md));
    }


    public void testSeal() {
        MutableDigest tmp=new MutableDigest(3);
        tmp.add(a2, 1,2,3);
        Assert.assertEquals(1, tmp.size());
        tmp.seal();
        try {
            tmp.add(a2, 4,5,6);
            assert false : "should run into an exception";
        }
        catch(IllegalAccessError e) {
            System.out.println("received exception \"" + e.toString() + "\" - as expected");
        }
        Assert.assertEquals(1, tmp.size());
    }



    public void testSeal2() {
        md.incrementHighestDeliveredSeqno(a1);
        md.seal();
        try {
            md.incrementHighestDeliveredSeqno(a3);
            assert false : "should run into an exception";
        }
        catch(IllegalAccessError e) {
            System.out.println("received exception \"" + e.toString() + "\" - as expected");
        }

        MutableDigest tmp=new MutableDigest(md);
        tmp.incrementHighestDeliveredSeqno(a3);
    }


    public void testAdd() {
        Assert.assertEquals(3, md.size());
        md.add(a1, 100, 200, 201);
        Assert.assertEquals(3, md.size());
        md.add(Util.createRandomAddress(), 1,2,3);
        Assert.assertEquals(4, md.size());
    }


    public void testAddDigest() {
        Digest tmp=md.copy();
        md.add(tmp);
        Assert.assertEquals(3, md.size());
    }


    public void testAddDigest2() {
        MutableDigest tmp=new MutableDigest(4);
        tmp.add(Util.createRandomAddress(), 1,2,3);
        tmp.add(Util.createRandomAddress(), 1,2,3);
        tmp.add(a2, 1,2,3);
        tmp.add(a3, 1,2,3);
        md.add(tmp);
        Assert.assertEquals(5, md.size());
    }


    public void testGet() {
        Digest.Entry entry;
        entry=d.get(a1);
        Assert.assertEquals(entry, new Digest.Entry(4, 500, 501));
        entry=d.get(a2);
        Assert.assertEquals(entry, new Digest.Entry(25, 26, 26));
        entry=d.get(a3);
        Assert.assertEquals(entry, new Digest.Entry(20, 25, 33));
    }


    public void testIncrementHighSeqno() {
        md=new MutableDigest(3);
        md.add(a1, 1, 100);
        md.add(a2, 3, 300);
        md.add(a3, 7, 700);

        long tmp=md.highestDeliveredSeqnoAt(a1);
        md.incrementHighestDeliveredSeqno(a1);
        Assert.assertEquals(md.highestDeliveredSeqnoAt(a1), tmp + 1);

        tmp=md.highestDeliveredSeqnoAt(a2);
        md.incrementHighestDeliveredSeqno(a2);
        Assert.assertEquals(md.highestDeliveredSeqnoAt(a2), tmp + 1);

        tmp=md.highestDeliveredSeqnoAt(a3);
        md.incrementHighestDeliveredSeqno(a3);
        Assert.assertEquals(md.highestDeliveredSeqnoAt(a3), tmp + 1);
    }



    public void testConstructor() {
        Assert.assertEquals(3, md.size());
        md.clear();
        Assert.assertEquals(0, md.size());
        md.clear();
        Assert.assertEquals(0, md.size());
    }



    public static void testConstructor2() {
        Digest dd=new Digest(3);
        Assert.assertEquals(0, dd.size());
    }


    public static void testConstructor3() {
        Digest dd=new MutableDigest(3);
        Assert.assertEquals(0, dd.size());
    }



    public void testContains() {
        assert d.contains(a1);
        assert d.contains(a2);
        assert d.contains(a3);
    }




    public void testResetAt() {
        md.resetAt(a1);
        Assert.assertEquals(0, md.lowSeqnoAt(a1));
        Assert.assertEquals(0, md.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(0, md.highestReceivedSeqnoAt(a1));
    }



    public void testLowSeqnoAt() {
        Assert.assertEquals(4, d.lowSeqnoAt(a1));
        Assert.assertEquals(25, d.lowSeqnoAt(a2));
        Assert.assertEquals(20, d.lowSeqnoAt(a3));
    }



    public void testHighSeqnoAt() {
        Assert.assertEquals(500, d.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(26, d.highestDeliveredSeqnoAt(a2));
        Assert.assertEquals(25, d.highestDeliveredSeqnoAt(a3));
    }

//    public void testSetHighSeqnoAt() {
//        assertEquals(500, md.highSeqnoAt(a1));
//        md.setHighSeqnoAt(a1, 555);
//        assertEquals(555, md.highSeqnoAt(a1));
//    }


    public void testHighSeqnoSeenAt() {
        Assert.assertEquals(501, d.highestReceivedSeqnoAt(a1));
        Assert.assertEquals(26, d.highestReceivedSeqnoAt(a2));
        Assert.assertEquals(33, d.highestReceivedSeqnoAt(a3));
    }

//    public void testSetHighSeenSeqnoAt() {
//        assertEquals(26, md.highSeqnoSeenAt(a2));
//        md.setHighSeqnoSeenAt(a2, 100);
//        assertEquals(100, md.highSeqnoSeenAt(a2));
//    }


    public void testSetHighestDeliveredAndSeenSeqnoAt() {
        Assert.assertEquals(4, d.lowSeqnoAt(a1));
        Assert.assertEquals(500, d.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(501, md.highestReceivedSeqnoAt(a1));
        md.setHighestDeliveredAndSeenSeqnos(a1, 2, 10, 20);
        Assert.assertEquals(2, md.lowSeqnoAt(a1));
        Assert.assertEquals(10, md.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(20, md.highestReceivedSeqnoAt(a1));
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
        Assert.assertEquals(tmp, d);
    }



    public void testMutableCopy() {
        Digest copy=md.copy();
        System.out.println("md=" + md + "\ncopy=" + copy);
        Assert.assertEquals(md, copy);
        md.add(a1, 4, 500, 1000);
        System.out.println("md=" + md + "\ncopy=" + copy);
        assert !(md.equals(copy));
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

        Assert.assertEquals(3, d.size());
        Assert.assertEquals(3, digest.size());

        Assert.assertEquals(3, digest.lowSeqnoAt(a1));
        Assert.assertEquals(500, digest.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(502, digest.highestReceivedSeqnoAt(a1));

        Assert.assertEquals(20, digest.lowSeqnoAt(a2));
        Assert.assertEquals(26, digest.highestDeliveredSeqnoAt(a2));
        Assert.assertEquals(27, digest.highestReceivedSeqnoAt(a2));

        Assert.assertEquals(20, digest.lowSeqnoAt(a3));
        Assert.assertEquals(26, digest.highestDeliveredSeqnoAt(a3));
        Assert.assertEquals(35, digest.highestReceivedSeqnoAt(a3));
    }


    public void testNonConflictingMerge() {
        MutableDigest cons_d=new  MutableDigest(5);
        Address ip1=Util.createRandomAddress(), ip2=Util.createRandomAddress();

        cons_d.add(ip1, 1, 10, 10);
        cons_d.add(ip2, 2, 20, 20);
        // System.out.println("\ncons_d before: " + cons_d);
        cons_d.merge(d);

        Assert.assertEquals(5, cons_d.size());
        //System.out.println("\ncons_d after: " + cons_d);
        Assert.assertEquals(1, cons_d.lowSeqnoAt(ip1));
        Assert.assertEquals(2, cons_d.lowSeqnoAt(ip2));
        Assert.assertEquals(4, cons_d.lowSeqnoAt(a1));
        Assert.assertEquals(25, cons_d.lowSeqnoAt(a2));
        Assert.assertEquals(20, cons_d.lowSeqnoAt(a3));

        Assert.assertEquals(10, cons_d.highestDeliveredSeqnoAt(ip1));
        Assert.assertEquals(20, cons_d.highestDeliveredSeqnoAt(ip2));
        Assert.assertEquals(500, cons_d.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(26, cons_d.highestDeliveredSeqnoAt(a2));
        Assert.assertEquals(25, cons_d.highestDeliveredSeqnoAt(a3));

        Assert.assertEquals(10, cons_d.highestReceivedSeqnoAt(ip1));
        Assert.assertEquals(20, cons_d.highestReceivedSeqnoAt(ip2));
        Assert.assertEquals(501, cons_d.highestReceivedSeqnoAt(a1));
        Assert.assertEquals(26, cons_d.highestReceivedSeqnoAt(a2));
        Assert.assertEquals(33, cons_d.highestReceivedSeqnoAt(a3));
    }



    public void testConflictingMerge() {
        MutableDigest new_d=new MutableDigest(2);
        new_d.add(a1, 5, 450, 501);
        new_d.add(a3, 18, 28, 35);
        //System.out.println("\nd before: " + d);
        //System.out.println("new_: " + new_d);
        md.merge(new_d);

        Assert.assertEquals(3, md.size());
        //System.out.println("d after: " + d);

        Assert.assertEquals(4, md.lowSeqnoAt(a1));
        Assert.assertEquals(500, md.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(501, md.highestReceivedSeqnoAt(a1));

        Assert.assertEquals(25, md.lowSeqnoAt(a2));
        Assert.assertEquals(26, md.highestDeliveredSeqnoAt(a2));
        Assert.assertEquals(26, md.highestReceivedSeqnoAt(a2));

        Assert.assertEquals(18, md.lowSeqnoAt(a3));
        Assert.assertEquals(28, md.highestDeliveredSeqnoAt(a3));
        Assert.assertEquals(35, md.highestReceivedSeqnoAt(a3));
    }



    public void testSameSendersOtherIsNull() {
        assert !(d.sameSenders(null));
    }


    public void testSameSenders1MNullDifferentLenth() {
        d2=new Digest(1);
        assert !(d2.sameSenders(d));
    }


    public void testSameSenders1MNullSameLength() {
        d2=new Digest(3);
        assert !(d2.sameSenders(d));
    }


    public void testSameSendersIdentical() {
        d2=d.copy();
        assert d.sameSenders(d2);
    }


    public void testSameSendersNotIdentical() {
        MutableDigest tmp=new MutableDigest(3);
        tmp.add(a1, 4, 500, 501);
        tmp.add(a3, 20, 25, 33);
        tmp.add(a2, 25, 26, 26);
        assert md.sameSenders(tmp);
        assert d.sameSenders(tmp);
    }


    public void testSameSendersNotSameLength() {
        md=new MutableDigest(3);
        md.add(a1, 4, 500, 501);
        md.add(a2, 25, 26, 26);
        assert !(d.sameSenders(md));
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
        Assert.assertEquals(d, tmp);
    }


    public void testSerializedSize() throws Exception {
        long len=d.serializedSize();
        byte[] buf=Util.streamableToByteBuffer(d);
        Assert.assertEquals(len, buf.length);
    }



}
