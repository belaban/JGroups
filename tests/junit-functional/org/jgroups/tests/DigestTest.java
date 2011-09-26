
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
 */
@Test(groups=Global.FUNCTIONAL, sequential=true)
public class DigestTest {
    Digest         d, d2;
    MutableDigest  md;
    Address        a1, a2, a3;


    @BeforeClass
    void beforeClass() throws Exception {
        a1=Util.createRandomAddress("a1");
        a2=Util.createRandomAddress("a2");
        a3=Util.createRandomAddress("a3");
    }

    @BeforeMethod
    void beforeMethod() {
        Map<Address, long[]> map=new HashMap<Address, long[]>();
        map.put(a1, new long[]{500, 501});
        map.put(a2, new long[]{26, 26});
        map.put(a3, new long[]{25, 33});
        d=new Digest(map);
        md=new MutableDigest(map);
    }


    public void testSize() {
        md=new MutableDigest(3);
        Assert.assertEquals(0, md.size());
    }


    public void testEquals() {
        d2=d.copy();
        System.out.println("d: " + d + "\nd2= " + d2);
        Assert.assertEquals(d, d);
        Assert.assertEquals(d, d2);
    }


    public void testDifference(){
		Map<Address, long[]> map=new HashMap<Address, long[]>();
	    map.put(a1, new long[]{500, 501});
	    map.put(a2, new long[]{26, 26});
	    map.put(a3, new long[]{25, 33});
	    Digest digest=new Digest(map);
	     
	    Map<Address, long[]> map2=new HashMap<Address, long[]>();       
	    map2.put(a1, new long[]{500, 501});
	    map2.put(a2, new long[]{26, 26});
	    map2.put(a3, new long[]{37, 33});
	    Digest digest2 =new Digest(map2);

        Assert.assertNotSame(digest, digest2);
	    
	    Digest diff = digest2.difference(digest);
	    System.out.println(diff);
        assert diff.contains(a3);
        Assert.assertEquals(1, diff.size());
	    
	    
	    Map<Address, long[]> map3=new HashMap<Address, long[]>();
	    map3.put(a1, new long[]{500, 501});
	    map3.put(a2, new long[]{26, 26});
	    map3.put(a3, new long[]{37, 33});
	    map3.put(Util.createRandomAddress(), new long[]{2, 3});
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
        Map<Address, long[]> map=new HashMap<Address, long[]>();
        map.put(a1, new long[]{500, 501});
        map.put(a2, new long[]{26, 26});
        map.put(a3, new long[]{25, 33});
        Digest my=new Digest(map);

        System.out.println("\nd: " + d + "\nmy: " + my);
        assert my.isGreaterThanOrEqual(d);

        map.remove(a3);
        map.put(a3, new long[]{26, 33});
        my=new Digest(map);
        System.out.println("\nd: " + d + "\nmy: " + my);
        assert my.isGreaterThanOrEqual(d);

        map.remove(a3);
        map.put(a3, new long[]{22, 32});
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
        MutableDigest copy=md.copy();
        Assert.assertEquals(copy, md);
        md.incrementHighestDeliveredSeqno(a1);
        assert !(copy.equals(md));
    }


    public void testResize() {
        md.add(Util.createRandomAddress("p"), 500, 510);
        md.add(Util.createRandomAddress("q"), 10, 10);
        md.add(Util.createRandomAddress("r"), 7, 10);
        assert md.size() == 6;
    }


    public void testSeal() {
        MutableDigest tmp=new MutableDigest(3);
        tmp.add(a2, 2,3);
        Assert.assertEquals(1, tmp.size());
        tmp.seal();
        try {
            tmp.add(a2, 5,6);
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
        md.add(a1, 200, 201);
        Assert.assertEquals(3, md.size());
        md.add(Util.createRandomAddress("p"), 2,3);
        Assert.assertEquals(4, md.size());
    }


    public void testAddDigest() {
        Digest tmp=md.copy();
        md.add(tmp);
        Assert.assertEquals(3, md.size());
    }


    public void testAddDigest2() {
        MutableDigest tmp=new MutableDigest(4);
        tmp.add(Util.createRandomAddress(), 2, 3);
        tmp.add(Util.createRandomAddress(), 2,3);
        tmp.add(a2, 2,3);
        tmp.add(a3, 2,3);
        md.add(tmp);
        Assert.assertEquals(5, md.size());
    }


    public void testGet() {
        long[] entry=d.get(a1);
        assert entry[0] ==500 && entry[1] == 501;

        entry=d.get(a2);
        assert entry[0] == 26 && entry[1] == 26;

        entry=d.get(a3);
        assert entry[0] == 25 && entry[1] == 33;
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
        Digest dd=new MutableDigest(3);
        Assert.assertEquals(0, dd.size());
    }


    public static void testConstructor3() {
        Digest dd=new MutableDigest(3);
        Assert.assertEquals(0, dd.size());
    }


    public void testCopyConstructor() {
        Digest digest=new Digest(d);
        assert digest.equals(d);
    }

     public void testCopyConstructorWithLargerDigest() {
         MutableDigest dig=new MutableDigest(10);
         dig.add(Util.createRandomAddress("A"), 1,1);
         dig.add(Util.createRandomAddress("B"), 2,2);
         dig.add(Util.createRandomAddress("C"), 3,3);

         Digest digest=new Digest(dig);
         assert digest.equals(dig);
    }


    public void testCopyConstructor2() {
        Digest digest=new Digest(md);
        assert digest.equals(md);
    }


    public void testCopyConstructor3() {
        MutableDigest digest=new MutableDigest(d);
        assert digest.equals(d);
    }


    public void testCopyConstructor4() {
        MutableDigest digest=new MutableDigest(md);
        assert digest.equals(md);
    }


    public void testContains() {
        assert d.contains(a1);
        assert d.contains(a2);
        assert d.contains(a3);
    }


    public void testContainsAll() {
        MutableDigest digest=new MutableDigest(5);
        digest.add(a1, 1, 1);
        digest.add(a2, 1, 1);

        assert d.containsAll(digest);
        assert !digest.containsAll(d);

        digest.add(a3, 1, 1);

        assert d.containsAll(digest);
        assert digest.containsAll(d);

        digest.add(Util.createRandomAddress("p"), 2, 2);

        assert !d.containsAll(digest);
        assert digest.containsAll(d);
    }


    public void testHighSeqnoAt() {
        Assert.assertEquals(500, d.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(26, d.highestDeliveredSeqnoAt(a2));
        Assert.assertEquals(25, d.highestDeliveredSeqnoAt(a3));
    }


    public void testReplace() {
        MutableDigest digest=new MutableDigest(2);
        digest.add(Util.createRandomAddress("x"), 1,1);
        digest.add(Util.createRandomAddress("y"), 2,2);

        md.replace(digest);
        assert digest.equals(md);
    }


    public void testHighSeqnoSeenAt() {
        Assert.assertEquals(501, d.highestReceivedSeqnoAt(a1));
        Assert.assertEquals(26, d.highestReceivedSeqnoAt(a2));
        Assert.assertEquals(33, d.highestReceivedSeqnoAt(a3));
    }


    public void testSetHighestDeliveredAndSeenSeqnoAt() {
        Assert.assertEquals(500, d.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(501, md.highestReceivedSeqnoAt(a1));
        md.setHighestDeliveredAndSeenSeqnos(a1, 10, 20);
        Assert.assertEquals(10, md.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(20, md.highestReceivedSeqnoAt(a1));
    }


    public void testCopy() {
        d=d.copy();
        testHighSeqnoAt();
        testHighSeqnoSeenAt();
        testContains();
    }



    public void testCopy2() {
        Digest tmp=d.copy();
        Assert.assertEquals(tmp, d);
    }



    public void testMutableCopy() {
        Digest copy=md.copy();
        System.out.println("md=" + md + "\ncopy=" + copy);
        Assert.assertEquals(md, copy);
        md.add(Util.createRandomAddress("p"), 500, 1000);
        System.out.println("md=" + md + "\ncopy=" + copy);
        assert !(md.equals(copy));
    }



    public void testMerge() {
        Map<Address, long[]> map=new HashMap<Address, long[]>();
        map.put(a1, new long[]{499, 502});
        map.put(a2, new long[]{26, 27});
        map.put(a3, new long[]{26, 35});
        MutableDigest digest=new MutableDigest(map);

        System.out.println("d: " + d);
        System.out.println("digest: " + digest);
        
        digest.merge(d);
        System.out.println("merged digest: " + digest);

        Assert.assertEquals(3, d.size());
        Assert.assertEquals(3, digest.size());

        Assert.assertEquals(500, digest.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(502, digest.highestReceivedSeqnoAt(a1));

        Assert.assertEquals(26, digest.highestDeliveredSeqnoAt(a2));
        Assert.assertEquals(27, digest.highestReceivedSeqnoAt(a2));

        Assert.assertEquals(26, digest.highestDeliveredSeqnoAt(a3));
        Assert.assertEquals(35, digest.highestReceivedSeqnoAt(a3));
    }


    public void testNonConflictingMerge() {
        MutableDigest cons_d=new  MutableDigest(5);
        Address ip1=Util.createRandomAddress("x"), ip2=Util.createRandomAddress("y");

        cons_d.add(ip1, 10, 10);
        cons_d.add(ip2, 20, 20);
        // System.out.println("\ncons_d before: " + cons_d);
        cons_d.merge(d);

        Assert.assertEquals(5, cons_d.size());

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
        new_d.add(a1, 450, 501);
        new_d.add(a3, 28, 35);
        //System.out.println("\nd before: " + d);
        //System.out.println("new_: " + new_d);
        md.merge(new_d);

        Assert.assertEquals(3, md.size());
        //System.out.println("d after: " + d);

        Assert.assertEquals(500, md.highestDeliveredSeqnoAt(a1));
        Assert.assertEquals(501, md.highestReceivedSeqnoAt(a1));

        Assert.assertEquals(26, md.highestDeliveredSeqnoAt(a2));
        Assert.assertEquals(26, md.highestReceivedSeqnoAt(a2));

        Assert.assertEquals(28, md.highestDeliveredSeqnoAt(a3));
        Assert.assertEquals(35, md.highestReceivedSeqnoAt(a3));
    }



    public void testSameSendersOtherIsNull() {
        assert !(d.sameSenders(null));
    }


    public void testSameSenders1MNullDifferentLenth() {
        d2=new MutableDigest(1);
        assert !(d2.sameSenders(d));
    }


    public void testSameSenders1MNullSameLength() {
        d2=new MutableDigest(3);
        assert !(d2.sameSenders(d));
    }


    public void testSameSendersIdentical() {
        d2=d.copy();
        assert d.sameSenders(d2);
    }


    public void testSameSendersNotIdentical() {
        MutableDigest tmp=new MutableDigest(3);
        tmp.add(a1, 500, 501);
        tmp.add(a3, 25, 33);
        tmp.add(a2, 26, 26);
        assert md.sameSenders(tmp);
        assert d.sameSenders(tmp);
    }


    public void testSameSendersNotSameLength() {
        md=new MutableDigest(3);
        md.add(a1, 500, 501);
        md.add(a2, 26, 26);
        assert !(d.sameSenders(md));
    }



    public void testStreamable() throws Exception {
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
