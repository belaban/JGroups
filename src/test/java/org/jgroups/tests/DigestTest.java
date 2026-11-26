
package org.jgroups.tests;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.View;
import org.jgroups.util.Digest;
import org.jgroups.util.MutableDigest;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Arrays;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL, singleThreaded=true)
public class DigestTest {
    protected Digest         d, d2;
    protected MutableDigest  md;
    protected Address        a1, a2, a3;
    protected Address[]      members;


    @BeforeClass void beforeClass() throws Exception {
        a1=Util.createRandomAddress("a1");
        a2=Util.createRandomAddress("a2");
        a3=Util.createRandomAddress("a3");
        members=new Address[]{a1,a2,a3};
    }

    @BeforeMethod void beforeMethod() {
        d=new Digest(members, new long[]{500,501, 26,26, 25,33});
        md=new MutableDigest(members);
    }


    public void testConstructor1() {
        Digest dd=new MutableDigest(members);
        Assert.assertEquals(dd.capacity(),members.length);
    }

    public void testConstructor2() {
        d=new Digest(members, new long[]{1,2,3,4,5,6});
        System.out.println("d = " + d);
        try {
            d=new Digest(members, new long[]{1,2,3,4,5,6,7});
            assert false : "seqnos of length 7 should be thrown an exception";
        }
        catch(IllegalArgumentException ex) {
            System.out.println("caught exception as expected: " + ex);
        }
    }

    public void testCopyConstructor() {
        Digest digest=new Digest(d);
        assert digest.equals(d);
    }

    public void testCopyConstructor2() {
        MutableDigest digest=new MutableDigest(d);
        assert digest.equals(d);
    }

    public void testCopyConstructor3() {
        MutableDigest digest=new MutableDigest(md);
        assert digest.equals(md);
    }

    public void testCapacity() {
        Assert.assertEquals(md.capacity(), 3);
        Assert.assertEquals(d.capacity(),  3);
    }


    public void testViewId() throws Exception {
        byte[] buf=Util.streamableToByteBuffer(d);
        Digest digest=Util.streamableFromByteBuffer(Digest::new,buf);
        System.out.println("digest = " + digest);
    }

    public void testContains() {
        assert d.contains(a1) && d.contains(a2) && d.contains(a3);
    }

    public void testContainsAll() {
        MutableDigest digest=new MutableDigest(members);
        digest.set(a1,1,1);
        digest.set(a2,1,1);
        assert digest.containsAll(a1, a2, a3);
        digest.set(a3,1,1);
        assert digest.containsAll(a1, a2, a3);
        assert d.containsAll(a1, a2, a3);
    }

    public void testEquals() {
        d2=d.copy();
        System.out.println("d: " + d + "\nd2= " + d2);
        Assert.assertEquals(d, d);
        Assert.assertEquals(d, d2);
    }

    public void testEquals2() {
        md=new MutableDigest(d);
        System.out.println("d: " + d + "\nmd= " + md);
        Assert.assertEquals(d, d);
        Assert.assertEquals(d,md);
        System.out.println("d: " + d + "\nmd= " + md);

        md=new MutableDigest(members).set(a1, 1,2).set(a2, 3,4);
        Assert.assertNotEquals(d,md);
    }

    public void testEquals3() {
        Digest digest=d;
        Assert.assertEquals(d,digest);

        digest=new Digest(members, new long[]{500,501, 26,26, 25,33});
        Assert.assertEquals(d, digest);

        digest=new Digest(members, new long[]{500,501, 26,26, 25,37});
        Assert.assertNotEquals(d,digest);

        digest=new MutableDigest(members).set(a1, 500,501).set(a2, 26,26);
        Assert.assertNotEquals(d,digest);
    }



    public void testImmutability2() {
        Digest tmp=d.copy();
        Assert.assertEquals(d, tmp);
    }


    public void testImmutability3() {
        Digest tmp=new Digest(d);
        Assert.assertEquals(tmp, d);
    }


    public void testSet() {
        md.set(a2,200,201);
        long[] seqnos=md.get(a2);
        Assert.assertEquals(seqnos[0], 200);
        Assert.assertEquals(seqnos[1], 201);
    }

    public void testImmutablity() {
        md=new MutableDigest(d);
        System.out.println("d = " + d);
        System.out.println("md = " + md);
        long[] before=d.get(a1);
        md.set(a1, 1, 1);
        System.out.println("d = " + d);
        System.out.println("md = " + md);
        long[] after=d.get(a1);
        Assert.assertEquals(after[0], before[0]);
        Assert.assertEquals(after[1], before[1]);
    }


    public void testSetDigest() {
        Digest tmp=md.copy();
        md.set(tmp);
        Assert.assertEquals(md.capacity(), 3);
    }


    public void testSetDigest2() {
        MutableDigest tmp=new MutableDigest(members);
        tmp.set(Util.createRandomAddress(),2,3); // ignored as view doesn't include this member
        tmp.set(Util.createRandomAddress(),2,3); // ditto
        tmp.set(a2,2,3);
        tmp.set(a3,2,3);
        md.set(tmp);
        Assert.assertEquals(md.capacity(), 3);
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
        md=new MutableDigest(members);
        md.set(a1,1,100).set(a2,3,300).set(a3,7,700);

        long tmp=md.get(a1)[0];
        md.set(a1,tmp + 1,tmp + 1);
        Assert.assertEquals(md.get(a1)[0], tmp + 1);

        tmp=md.get(a2)[0];
        md.set(a2,tmp + 1,tmp + 1);
        Assert.assertEquals(md.get(a2)[0], tmp + 1);

        tmp=md.get(a3)[0];
        md.set(a3,tmp + 1,tmp + 1);
        Assert.assertEquals(md.get(a3)[0], tmp + 1);
    }

    public void testHighSeqnoAt() {
        Assert.assertEquals(d.get(a1)[0], 500);
        Assert.assertEquals(d.get(a2)[0], 26);
        Assert.assertEquals(d.get(a3)[0], 25);
    }


    public void testReplace() {
        MutableDigest digest=new MutableDigest(members);
        digest.set(a1,1,1);
        digest.set(a2,2,2);

        md.set(digest);
        assert digest.equals(md);
    }


    public void testHighSeqnoSeenAt() {
        Assert.assertEquals(d.get(a1)[1], 501);
        Assert.assertEquals(d.get(a2)[1], 26);
        Assert.assertEquals(d.get(a3)[1], 33);
    }


    public void testSetHighestDeliveredAndSeenSeqnoAt() {
        md=new MutableDigest(d);
        Assert.assertEquals(md.get(a1)[0], 500);
        Assert.assertEquals(md.get(a1)[1], 501);
        md.set(a1,10,20);
        Assert.assertEquals(md.get(a1)[0], 10);
        Assert.assertEquals(md.get(a1)[1], 20);
    }


    public void testCopy() {
        d=d.copy();
        testHighSeqnoAt();
        testHighSeqnoSeenAt();
        testContains();
    }


    public void testCopy2() {
        assert d.equals(d.copy());
    }


    public void testMutableCopy() {
        Digest copy=md.copy();
        System.out.println("md=" + md + "\ncopy=" + copy);
        Assert.assertEquals(md, copy);
        md.set(Util.createRandomAddress("p"),500,1000); // is ignored as p is not a member
        System.out.println("md=" + md + "\ncopy=" + copy);
        assert md.equals(copy);
    }


    public void testAllSet() {
        Address[] mbrs=Util.createRandomAddresses(5);
        MutableDigest dig=new MutableDigest(mbrs);
        assert !dig.allSet();
        for(int index: Arrays.asList(1,3,4))
            dig.set(mbrs[index], index, index+1);
        System.out.println("dig = " + dig);
        assert !dig.allSet();

        Address[] non_set=dig.getNonSetMembers();
        System.out.println("non_set = " + Arrays.toString(non_set));
        assert non_set != null && non_set.length == 2;
        Assert.assertEquals(non_set, new Address[]{mbrs[0], mbrs[2]});


        for(int index: Arrays.asList(0,2))
            dig.set(mbrs[index], index, index+1);
        System.out.println("dig = " + dig);
        assert dig.allSet();
        non_set=dig.getNonSetMembers();
        assert non_set.length == 0;

        dig=new MutableDigest(members);
        assert !dig.allSet();
        non_set=dig.getNonSetMembers();
        Assert.assertEquals(non_set, members);

        dig.set(d);
        System.out.println("dig = " + dig);
        assert dig.allSet();
        non_set=dig.getNonSetMembers();
        assert non_set.length == 0;
    }


    public void testMerge() {
        MutableDigest digest=new MutableDigest(members)
          .set(a1, 499,502).set(a2, 26,27).set(a3, 26,35);

        System.out.println("d: " + d);
        System.out.println("digest: " + digest);
        
        digest.merge(d);
        System.out.println("merged digest: " + digest);

        Assert.assertEquals(d.capacity(), 3);
        Assert.assertEquals(digest.capacity(), 3);

        Assert.assertEquals(digest.get(a1)[0], 500);
        Assert.assertEquals(digest.get(a1)[1], 502);

        Assert.assertEquals(digest.get(a2)[0], 26);
        Assert.assertEquals(digest.get(a2)[1], 27);

        Assert.assertEquals(digest.get(a3)[0], 26);
        Assert.assertEquals(digest.get(a3)[1], 35);
    }


    public void testNonConflictingMerge() {
        Address ip1=Util.createRandomAddress("x"), ip2=Util.createRandomAddress("y");
        View tmp_view=View.create(a1,1,a1,a2,a3,ip1,ip2);
        MutableDigest cons_d=new MutableDigest(tmp_view.getMembersRaw());

        cons_d.set(ip1,10,10);
        cons_d.set(ip2,20,20);
        cons_d.merge(d);

        Assert.assertEquals(cons_d.capacity(), 5);

        Assert.assertEquals(cons_d.get(ip1)[0], 10);
        Assert.assertEquals(cons_d.get(ip2)[0], 20);
        Assert.assertEquals(cons_d.get(a1)[0], 500);
        Assert.assertEquals(cons_d.get(a2)[0], 26);
        Assert.assertEquals(cons_d.get(a3)[0], 25);

        Assert.assertEquals(cons_d.get(ip1)[1], 10);
        Assert.assertEquals(cons_d.get(ip2)[1], 20);
        Assert.assertEquals(cons_d.get(a1)[1], 501);
        Assert.assertEquals(cons_d.get(a2)[1], 26);
        Assert.assertEquals(cons_d.get(a3)[1], 33);
    }



    public void testConflictingMerge() {
        MutableDigest new_d=new MutableDigest(members);
        new_d.set(a1,450,501);
        new_d.set(a3,28,35);
        md=new MutableDigest(d);
        md.merge(new_d);

        Assert.assertEquals(md.capacity(), 3);

        Assert.assertEquals(md.get(a1)[0], 500);
        Assert.assertEquals(md.get(a1)[1], 501);

        Assert.assertEquals(md.get(a2)[0], 26);
        Assert.assertEquals(md.get(a2)[1], 26);

        Assert.assertEquals(md.get(a3)[0], 28);
        Assert.assertEquals(md.get(a3)[1], 35);
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
        long len=d.serializedSize(true);
        byte[] buf=Util.streamableToByteBuffer(d);
        Assert.assertEquals(buf.length, len);
    }


    public void testViewBasedMarshalling() throws Exception {
        byte[] buf=Util.streamableToByteBuffer(d);
        Digest new_digest=Util.streamableFromByteBuffer(Digest::new,buf);
        System.out.println("new_digest = " + new_digest);
        assert new_digest.equals(d);
    }

    public void testViewBasedMarshallingLargeView() throws Exception {
        final int DIGEST_SIZE=1000;
        long[] seqnos=new long[DIGEST_SIZE *2];

        Address[] mbrs=new Address[DIGEST_SIZE];
        for(int i=0; i < DIGEST_SIZE; i++)
            mbrs[i]=Util.createRandomAddress(String.valueOf(i));

        for(int i=0; i < DIGEST_SIZE; i++) {
            int hd=(int)Util.random(Integer.MAX_VALUE);
            seqnos[i*2]=hd;
            seqnos[i*2 +1]=hd + Util.random(1000);
        }

        Digest digest=new Digest(mbrs, seqnos);

        byte[] buf1=Util.streamableToByteBuffer(digest);
        System.out.println("buf1: " + buf1.length + " bytes");

        Digest digest1=Util.streamableFromByteBuffer(Digest::new,buf1);

        System.out.println("digest1 = " + digest1);
        assert digest.equals(digest1);
        assert digest.capacity() == digest1.capacity();

    }


}
