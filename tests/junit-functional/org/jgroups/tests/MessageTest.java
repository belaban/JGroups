
package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.UDP;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.pbcast.NakAckHeader;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.util.Range;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class MessageTest {

    static final short UDP_ID=101;
    static final short PING_ID=102;
    static final short NAKACK_ID=103;


    public static void testFlags() {
        Message m1=new Message();
        assert !(m1.isFlagSet(Message.OOB));
        try {
            m1.setFlag((byte)1002);
            assert false : "1002 is not a byte value";
        }
        catch(IllegalArgumentException ex) {
        }
        assert m1.getFlags() == 0;
    }


    public static void testFlags2() {
        Message m1=new Message();
        m1.setFlag(Message.OOB);
        assert m1.isFlagSet(Message.OOB);
        assert Message.OOB == (m1.getFlags() & Message.OOB);
        assert !(m1.isFlagSet(Message.DONT_BUNDLE));
        Assert.assertNotSame((m1.getFlags() & Message.DONT_BUNDLE), Message.DONT_BUNDLE);
    }

    public static void testFlags3() {
        Message msg=new Message();
        assert msg.isFlagSet(Message.OOB) == false;
        msg.setFlag(Message.OOB);
        assert msg.isFlagSet(Message.OOB);
        msg.setFlag(Message.OOB);
        assert msg.isFlagSet(Message.OOB);
    }


    public static void testClearFlags() {
        Message msg=new Message();
        msg.setFlag(Message.OOB);
        assert msg.isFlagSet(Message.OOB);
        msg.clearFlag(Message.OOB);
        assert msg.isFlagSet(Message.OOB) == false;
        msg.clearFlag(Message.OOB);
        assert msg.isFlagSet(Message.OOB) == false;
        msg.setFlag(Message.OOB);
        assert msg.isFlagSet(Message.OOB);
    }


    public static void testClearFlags2() {
        Message msg=new Message();
        msg.setFlag(Message.OOB);
        msg.setFlag(Message.NO_FC);
        assert msg.isFlagSet(Message.DONT_BUNDLE) == false;
        assert msg.isFlagSet(Message.OOB);
        assert msg.isFlagSet(Message.NO_FC);

        msg.clearFlag(Message.OOB);
        assert msg.isFlagSet(Message.OOB) == false;
        msg.setFlag(Message.DONT_BUNDLE);
        assert msg.isFlagSet(Message.DONT_BUNDLE);
        assert msg.isFlagSet(Message.NO_FC);
        msg.clearFlag(Message.NO_FC);
        assert msg.isFlagSet(Message.NO_FC) == false;
        msg.clearFlag(Message.NO_FC);
        assert msg.isFlagSet(Message.NO_FC) == false;
        msg.clearFlag(Message.DONT_BUNDLE);
        msg.clearFlag(Message.OOB);
        assert msg.getFlags() == 0;
        assert msg.isFlagSet(Message.OOB) == false;
        assert msg.isFlagSet(Message.DONT_BUNDLE) == false;
        assert msg.isFlagSet(Message.NO_FC) == false;
        msg.setFlag(Message.DONT_BUNDLE);
        assert msg.isFlagSet(Message.DONT_BUNDLE);
        msg.setFlag(Message.DONT_BUNDLE);
        assert msg.isFlagSet(Message.DONT_BUNDLE);
    }


    public static void testBufferSize() throws Exception {
        Message m1=new Message(null, null, "bela");
        assert m1.getRawBuffer() != null;
        assert m1.getBuffer() != null;
        Assert.assertEquals(m1.getBuffer().length, m1.getLength());
        byte[] new_buf={'m', 'i', 'c', 'h', 'e', 'l', 'l', 'e'};
        m1.setBuffer(new_buf);
        assert m1.getRawBuffer() != null;
        assert m1.getBuffer() != null;
        Assert.assertEquals(new_buf.length, m1.getLength());
        Assert.assertEquals(m1.getBuffer().length, m1.getLength());
    }


    public static void testBufferOffset() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new Message(null, null, buf, 0, 4);
        Message m2=new Message(null, null, buf, 4, 3);

        byte[] b1, b2;

        b1=new byte[m1.getLength()];
        System.arraycopy(m1.getRawBuffer(), m1.getOffset(), b1, 0, m1.getLength());

        b2=new byte[m2.getLength()];
        System.arraycopy(m2.getRawBuffer(), m2.getOffset(), b2, 0, m2.getLength());

        Assert.assertEquals(4, b1.length);
        Assert.assertEquals(3, b2.length);
    }



    public static void testSetBufferWithNullBuffer() {
        byte[] buf={'b', 'e', 'l', 'a'};
        Message m1=new Message();
        m1.setBuffer(buf, 1, 2); // dummy data with non 0 oiffset and length
        Assert.assertEquals(1, m1.getOffset());
        Assert.assertEquals(2, m1.getLength());

        m1.setBuffer(null, 1, 2); // dummy offset and length, is ignored
        Assert.assertEquals(0, m1.getOffset());
        Assert.assertEquals(0, m1.getLength());
    }


    @Test(groups=Global.FUNCTIONAL, expectedExceptions=ArrayIndexOutOfBoundsException.class)
    public static void testInvalidOffset() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new Message(null, null, buf, -1, 4);
        System.out.println("message is " + m1);
    }

    @Test(groups=Global.FUNCTIONAL, expectedExceptions=ArrayIndexOutOfBoundsException.class)
    public static void testInvalidLength() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new Message(null, null, buf, 3, 6);
        System.out.println("we should not get here with " + m1);
    }


    public static void testGetRawBuffer() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new Message(null, null, buf, 0, 4);
        Message m2=new Message(null, null, buf, 4, 3);

        Assert.assertEquals(buf.length, m1.getRawBuffer().length);
        Assert.assertEquals(4, m1.getBuffer().length);
        Assert.assertEquals(4, m1.getLength());

        Assert.assertEquals(buf.length, m2.getRawBuffer().length);
        Assert.assertEquals(3, m2.getBuffer().length);
        Assert.assertEquals(3, m2.getLength());
    }



    public static void testSetObject() {
        String s1="Bela Ban";
        Message m1=new Message(null, null, s1);
        Assert.assertEquals(0, m1.getOffset());
        Assert.assertEquals(m1.getBuffer().length, m1.getLength());
        String s2=(String)m1.getObject();
        Assert.assertEquals(s2, s1);
    }




    public static void testCopy() {
        Message m1=new Message(null, null, "Bela Ban");
        Message m2=m1.copy();
        Assert.assertEquals(m1.getOffset(), m2.getOffset());
        Assert.assertEquals(m1.getLength(), m2.getLength());
    }



    public static void testCopyWithOffset() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new Message(null, null, buf, 0, 4);
        Message m2=new Message(null, null, buf, 4, 3);

        Message m3, m4;
        m3=m1.copy();
        m4=m2.copy();

        Assert.assertEquals(0, m3.getOffset());
        Assert.assertEquals(4, m3.getLength());
        Assert.assertEquals(4, m3.getBuffer().length);

        Assert.assertEquals(4, m4.getOffset());
        Assert.assertEquals(3, m4.getLength());
        Assert.assertEquals(3, m4.getBuffer().length);
    }


    public static void testComputeFragOffsets() {
        Range r;
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List retval=Util.computeFragOffsets(buf, 4);
        System.out.println("list is " + retval);
        Assert.assertEquals(3, retval.size());
        r=(Range)retval.get(0);
        Assert.assertEquals(0, r.low);
        Assert.assertEquals(4, r.high);

        r=(Range)retval.get(1);
        Assert.assertEquals(4, r.low);
        Assert.assertEquals(4, r.high);

        r=(Range)retval.get(2);
        Assert.assertEquals(8, r.low);
        Assert.assertEquals(2, r.high);
    }



    public static void testComputeFragOffsetsWithOffsets() {
        Range r;
        // byte[] buf={'p', 'a', 'd', 0,1,2,3,4,5,6,7,8,9, 'p', 'a', 'd', 'd', 'i', 'e'};
        java.util.List retval=Util.computeFragOffsets(3, 10, 4);
        System.out.println("list is " + retval);
        Assert.assertEquals(3, retval.size());
        r=(Range)retval.get(0);
        Assert.assertEquals(3, r.low);
        Assert.assertEquals(4, r.high);

        r=(Range)retval.get(1);
        Assert.assertEquals(7, r.low);
        Assert.assertEquals(4, r.high);

        r=(Range)retval.get(2);
        Assert.assertEquals(11, r.low);
        Assert.assertEquals(2, r.high);
    }


    public static void testComputeFragOffsets2() {
        Range r;
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List retval=Util.computeFragOffsets(buf, 10);
        System.out.println("list is " + retval);
        Assert.assertEquals(1, retval.size());
        r=(Range)retval.get(0);
        Assert.assertEquals(0, r.low);
        Assert.assertEquals(10, r.high);
    }


    public static void testComputeFragOffsets3() {
        Range r;
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List retval=Util.computeFragOffsets(buf, 100);
        System.out.println("list is " + retval);
        Assert.assertEquals(1, retval.size());
        r=(Range)retval.get(0);
        Assert.assertEquals(0, r.low);
        Assert.assertEquals(10, r.high);
    }


    public static void testComputeFragOffsets4() {
        Range r;
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List retval=Util.computeFragOffsets(buf, 5);
        System.out.println("list is " + retval);
        Assert.assertEquals(2, retval.size());
        r=(Range)retval.get(0);
        Assert.assertEquals(0, r.low);
        Assert.assertEquals(5, r.high);

        r=(Range)retval.get(1);
        Assert.assertEquals(5, r.low);
        Assert.assertEquals(5, r.high);
    }



    public static void testSizeNullMessage() throws Exception {
        Message msg=new Message();
        _testSize(msg);
    }


    public static void testSizeMessageWithDest() throws Exception {
        Message msg=new Message(UUID.randomUUID(), null, null);
        _testSize(msg);
    }


    public static void testSizeMessageWithSrc() throws Exception {
        Message msg=new Message(null, UUID.randomUUID(), null);
        _testSize(msg);
    }


    public static void testSizeMessageWithDestAndSrc() throws Exception {
        Message msg=new Message(UUID.randomUUID(), UUID.randomUUID(), null);
        _testSize(msg);
    }



    public static void testSizeMessageWithDestAndSrcAndFlags() throws Exception {
        Message msg=new Message(UUID.randomUUID(), UUID.randomUUID(), null);
        msg.setFlag(Message.OOB);
        msg.setFlag(Message.DONT_BUNDLE);
        _testSize(msg);
    }


    public static void testSizeMessageWithBuffer() throws Exception {
        Message msg=new Message(null, null, "bela".getBytes());
        _testSize(msg);
    }


    public static void testSizeMessageWithBuffer2() throws Exception {
        Message msg=new Message(null, null, new byte[]{'b', 'e', 'l', 'a'});
        _testSize(msg);
    }


    public static void testSizeMessageWithBuffer3() throws Exception {
        Message msg=new Message(null, null, "bela");
        _testSize(msg);
    }


    public static void testSizeMessageWithAdditionalData() throws Exception {
        UUID dest=UUID.randomUUID();
        dest.setAdditionalData("bela".getBytes());
        Message msg=new Message(dest, null, null);
        _testSize(msg);
    }


    public static void testSizeMessageWithDestAndSrcAndHeaders() throws Exception {
        Message msg=new Message(UUID.randomUUID(), UUID.randomUUID(), "bela".getBytes());
        addHeaders(msg);
        _testSize(msg);
    }

    private static void addHeaders(Message msg) {       
        TpHeader tp_hdr=new TpHeader("DemoChannel2");
        msg.putHeader(UDP_ID, tp_hdr);
        PingHeader ping_hdr=new PingHeader(PingHeader.GET_MBRS_REQ, "demo-cluster");
        msg.putHeader(PING_ID, ping_hdr);
        NakAckHeader nak_hdr=NakAckHeader.createXmitRequestHeader(100, 104, null);
        msg.putHeader(NAKACK_ID, nak_hdr);
    }


    private static void _testSize(Message msg) throws Exception {
        long size=msg.size();
        byte[] serialized_form=Util.streamableToByteBuffer(msg);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(size, serialized_form.length);
    }


}
