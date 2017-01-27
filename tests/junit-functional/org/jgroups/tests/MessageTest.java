
package org.jgroups.tests;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.Header;
import org.jgroups.Message;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.util.ByteArrayDataInputStream;
import org.jgroups.util.Range;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.Map;
import java.util.function.Supplier;

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
        assert !m1.isFlagSet(Message.Flag.OOB);
        assert m1.getFlags() == 0;

        m1.setFlag((Message.Flag[])null);

        assert !m1.isFlagSet(Message.Flag.OOB);
        assert !m1.isFlagSet(null);
    }


    public void testSettingMultipleFlags() {
        Message msg=new Message();
        msg.setFlag((Message.Flag[])null);
        assert msg.getFlags() == 0;

        msg.setFlag(Message.Flag.OOB,Message.Flag.NO_FC, null, Message.Flag.DONT_BUNDLE);
        assert msg.isFlagSet(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.NO_FC);
        assert msg.isFlagSet(Message.Flag.DONT_BUNDLE);
    }


    public static void testFlags2() {
        Message m1=new Message();
        m1.setFlag(Message.Flag.OOB);
        assert m1.isFlagSet(Message.Flag.OOB);
        assert Message.isFlagSet(m1.getFlags(), Message.Flag.OOB);
        assert !(m1.isFlagSet(Message.Flag.DONT_BUNDLE));
        assert !Message.isFlagSet(m1.getFlags(), Message.Flag.DONT_BUNDLE);
    }

    public static void testFlags3() {
        Message msg=new Message();
        assert !msg.isFlagSet(Message.Flag.OOB);
        msg.setFlag(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.OOB);
        msg.setFlag(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.OOB);
    }


    public static void testClearFlags() {
        Message msg=new Message();
        msg.setFlag(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.OOB);
        msg.clearFlag(Message.Flag.OOB);
        assert !msg.isFlagSet(Message.Flag.OOB);
        msg.clearFlag(Message.Flag.OOB);
        assert !msg.isFlagSet(Message.Flag.OOB);
        msg.setFlag(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.OOB);
    }


    public static void testClearFlags2() {
        Message msg=new Message();
        msg.setFlag(Message.Flag.OOB);
        msg.setFlag(Message.Flag.NO_FC);
        assert !msg.isFlagSet(Message.Flag.DONT_BUNDLE);
        assert msg.isFlagSet(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.NO_FC);

        msg.clearFlag(Message.Flag.OOB);
        assert !msg.isFlagSet(Message.Flag.OOB);
        msg.setFlag(Message.Flag.DONT_BUNDLE);
        assert msg.isFlagSet(Message.Flag.DONT_BUNDLE);
        assert msg.isFlagSet(Message.Flag.NO_FC);
        msg.clearFlag(Message.Flag.NO_FC);
        assert !msg.isFlagSet(Message.Flag.NO_FC);
        msg.clearFlag(Message.Flag.NO_FC);
        assert !msg.isFlagSet(Message.Flag.NO_FC);
        msg.clearFlag(Message.Flag.DONT_BUNDLE);
        msg.clearFlag(Message.Flag.OOB);
        assert msg.getFlags() == 0;
        assert !msg.isFlagSet(Message.Flag.OOB);
        assert !msg.isFlagSet(Message.Flag.DONT_BUNDLE);
        assert !msg.isFlagSet(Message.Flag.NO_FC);
        msg.setFlag(Message.Flag.DONT_BUNDLE);
        assert msg.isFlagSet(Message.Flag.DONT_BUNDLE);
        msg.setFlag(Message.Flag.DONT_BUNDLE);
        assert msg.isFlagSet(Message.Flag.DONT_BUNDLE);
    }

    public void testDontLoopback() {
        final Address DEST=Util.createRandomAddress("A");
        Message msg=new Message(null).setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);

        msg.dest(null); // OK
        msg.setDest(null);

        msg.dest(DEST);

        msg.clearTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
        msg.dest(DEST); // OK
        msg.setTransientFlag(Message.TransientFlag.DONT_LOOPBACK);
        msg.setTransientFlagIfAbsent(Message.TransientFlag.DONT_LOOPBACK);

        short flags=(short)(Message.TransientFlag.DONT_LOOPBACK.value() + Message.TransientFlag.OOB_DELIVERED.value());
        msg.setTransientFlag(flags);
    }


    public static void testBufferSize() throws Exception {
        Message m1=new Message(null, "bela");
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
        Message m1=new Message(null, buf, 0, 4);
        Message m2=new Message(null, buf, 4, 3);

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
        Message m1=new Message(null, buf, -1, 4);
        System.out.println("message is " + m1);
    }

    @Test(groups=Global.FUNCTIONAL, expectedExceptions=ArrayIndexOutOfBoundsException.class)
    public static void testInvalidLength() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new Message(null, buf, 3, 6);
        System.out.println("we should not get here with " + m1);
    }


    public static void testGetRawBuffer() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new Message(null, buf, 0, 4);
        Message m2=new Message(null, buf, 4, 3);

        Assert.assertEquals(buf.length, m1.getRawBuffer().length);
        Assert.assertEquals(4, m1.getBuffer().length);
        Assert.assertEquals(4, m1.getLength());

        Assert.assertEquals(buf.length, m2.getRawBuffer().length);
        Assert.assertEquals(3, m2.getBuffer().length);
        Assert.assertEquals(3, m2.getLength());
    }



    public static void testSetObject() {
        String s1="Bela Ban";
        Message m1=new Message(null, s1);
        Assert.assertEquals(0, m1.getOffset());
        Assert.assertEquals(m1.getBuffer().length, m1.getLength());
        String s2=m1.getObject();
        Assert.assertEquals(s2, s1);
    }


    public static void testCopy() {
        Message m1=new Message(null, "Bela Ban");
        m1.setFlag(Message.Flag.OOB);
        m1.setTransientFlag(Message.TransientFlag.OOB_DELIVERED);
        Message m2=m1.copy();
        Assert.assertEquals(m1.getOffset(), m2.getOffset());
        Assert.assertEquals(m1.getLength(), m2.getLength());
        assert m2.isFlagSet(Message.Flag.OOB);
        assert m2.isTransientFlagSet(Message.TransientFlag.OOB_DELIVERED);
    }



    public static void testCopyWithOffset() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new Message(null, buf, 0, 4);
        Message m2=new Message(null, buf, 4, 3);

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

    public static void testCopyHeaders() {
        Message m1=new Message(null, "hello");
        for(short id: new short[]{1, 2, 10, Global.BLOCKS_START_ID, Global.BLOCKS_START_ID +10}) {
            m1.putHeader(id, new DummyHeader(id));
        }
        System.out.println("Headers for m1: " + m1.printHeaders());

        Message m2=m1.copy(true, Global.BLOCKS_START_ID);
        System.out.println("Headers for m2: " + m2.printHeaders());
        Map<Short,Header> hdrs=m2.getHeaders();
        assert hdrs.size() == 2;
        assert hdrs.containsKey(Global.BLOCKS_START_ID);

        short tmp=Global.BLOCKS_START_ID +10;
        assert hdrs.containsKey(tmp);
    }


    public void testComputeFragOffsets() {
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List<Range> retval=Util.computeFragOffsets(buf, 4);
        System.out.println("list is " + retval);
        Assert.assertEquals(3, retval.size());
        Range r=retval.get(0);
        Assert.assertEquals(0, r.low);
        Assert.assertEquals(4, r.high);

        r=retval.get(1);
        Assert.assertEquals(4, r.low);
        Assert.assertEquals(4, r.high);

        r=retval.get(2);
        Assert.assertEquals(8, r.low);
        Assert.assertEquals(2, r.high);
    }



    public void testComputeFragOffsetsWithOffsets() {
        Range r;
        // byte[] buf={'p', 'a', 'd', 0,1,2,3,4,5,6,7,8,9, 'p', 'a', 'd', 'd', 'i', 'e'};
        java.util.List<Range> retval=Util.computeFragOffsets(3, 10, 4);
        System.out.println("list is " + retval);
        Assert.assertEquals(3, retval.size());
        r=retval.get(0);
        Assert.assertEquals(3, r.low);
        Assert.assertEquals(4, r.high);

        r=retval.get(1);
        Assert.assertEquals(7, r.low);
        Assert.assertEquals(4, r.high);

        r=retval.get(2);
        Assert.assertEquals(11, r.low);
        Assert.assertEquals(2, r.high);
    }


    public void testComputeFragOffsets2() {
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List<Range> retval=Util.computeFragOffsets(buf, 10);
        System.out.println("list is " + retval);
        Assert.assertEquals(1, retval.size());
        Range r=retval.get(0);
        Assert.assertEquals(0, r.low);
        Assert.assertEquals(10, r.high);
    }


    public void testComputeFragOffsets3() {
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List<Range> retval=Util.computeFragOffsets(buf, 100);
        System.out.println("list is " + retval);
        Assert.assertEquals(1, retval.size());
        Range r=retval.get(0);
        Assert.assertEquals(0, r.low);
        Assert.assertEquals(10, r.high);
    }


    public void testComputeFragOffsets4() {
        byte[] buf={0,1,2,3,4,5,6,7,8,9};
        java.util.List<Range> retval=Util.computeFragOffsets(buf, 5);
        System.out.println("list is " + retval);
        Assert.assertEquals(2, retval.size());
        Range r=retval.get(0);
        Assert.assertEquals(0, r.low);
        Assert.assertEquals(5, r.high);

        r=retval.get(1);
        Assert.assertEquals(5, r.low);
        Assert.assertEquals(5, r.high);
    }



    public static void testSizeNullMessage() throws Exception {
        Message msg=new Message();
        _testSize(msg);
    }


    public static void testSizeMessageWithDest() throws Exception {
        Message msg=new Message(UUID.randomUUID());
        _testSize(msg);
    }


    public static void testSizeMessageWithSrc() throws Exception {
        Message msg=new Message(null).src(UUID.randomUUID());
        _testSize(msg);
    }


    public static void testSizeMessageWithDestAndSrc() throws Exception {
        Message msg=new Message(UUID.randomUUID()).src(UUID.randomUUID());
        _testSize(msg);
    }



    public static void testSizeMessageWithDestAndSrcAndFlags() throws Exception {
        Message msg=new Message(UUID.randomUUID()).src(UUID.randomUUID());
        msg.setFlag(Message.Flag.OOB);
        msg.setFlag(Message.Flag.DONT_BUNDLE);
        _testSize(msg);
    }


    public static void testSizeMessageWithBuffer() throws Exception {
        Message msg=new Message(null, "bela".getBytes());
        _testSize(msg);
    }


    public static void testSizeMessageWithBuffer2() throws Exception {
        Message msg=new Message(null, new byte[]{'b', 'e', 'l', 'a'});
        _testSize(msg);
    }


    public static void testSizeMessageWithBuffer3() throws Exception {
        Message msg=new Message(null, "bela");
        _testSize(msg);
    }


    public void testSizeMessageWithDestAndSrcAndHeaders() throws Exception {
        Message msg=new Message(UUID.randomUUID(), "bela".getBytes()).src(UUID.randomUUID());
        addHeaders(msg);
        _testSize(msg);
    }

    public void testReadFromSkipPayload() throws Exception {
        Message msg=new Message(Util.createRandomAddress("A"), "bela".getBytes()).src(Util.createRandomAddress("B"));
        addHeaders(msg);
        byte[] buf=Util.streamableToByteBuffer(msg);

        // ExposedByteArrayInputStream input=new ExposedByteArrayInputStream(buf);
        // DataInput in=new DataInputStream(input);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf);

        Message msg2=new Message(false);
        int payload_position=msg2.readFromSkipPayload(in);
        msg2.setBuffer(buf, payload_position, buf.length - payload_position);
        assert msg2.getOffset() == payload_position;
        assert msg2.getLength() == msg.getLength();
        assert msg2.size() == msg.size();

        Message copy=msg2.copy();
        assert copy.getOffset() == payload_position;
        assert copy.getLength() == msg.getLength();
        assert copy.size() == msg2.size();
    }

    public static void testReadFromSkipPayloadNullPayload() throws Exception {
        Message msg=new Message(Util.createRandomAddress("A")).src(Util.createRandomAddress("B"));
        addHeaders(msg);
        byte[] buf=Util.streamableToByteBuffer(msg);

        // ExposedByteArrayInputStream input=new ExposedByteArrayInputStream(buf);
        // DataInput in=new DataInputStream(input);
        ByteArrayDataInputStream in=new ByteArrayDataInputStream(buf);
        Message msg2=new Message(false);
        int payload_position=msg2.readFromSkipPayload(in);
        if(payload_position >= 0)
            msg2.setBuffer(buf, payload_position, buf.length - payload_position);
        assert msg2.getOffset() == 0;
        assert msg2.getLength() == msg.getLength();
        assert msg.getRawBuffer() == null;
        assert msg2.getRawBuffer() == null;
        assert msg.getBuffer() == null;
        assert msg2.getBuffer() == null;
        assert msg2.size() == msg.size();

        Message copy=msg2.copy();
        assert copy.getOffset() == 0;
        assert copy.getLength() == msg.getLength();
        assert copy.getRawBuffer() == null;
        assert copy.getBuffer() == null;
        assert copy.size() == msg2.size();
    }

    protected static void addHeaders(Message msg) {
        TpHeader tp_hdr=new TpHeader("DemoChannel2");
        msg.putHeader(UDP_ID, tp_hdr);
        PingHeader ping_hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName("demo-cluster");
        msg.putHeader(PING_ID, ping_hdr);
        NakAckHeader2 nak_hdr=NakAckHeader2.createXmitRequestHeader(Util.createRandomAddress("S"));
        msg.putHeader(NAKACK_ID, nak_hdr);
    }


    private static void _testSize(Message msg) throws Exception {
        long size=msg.size();
        byte[] serialized_form=Util.streamableToByteBuffer(msg);
        System.out.println("size=" + size + ", serialized size=" + serialized_form.length);
        Assert.assertEquals(size, serialized_form.length);
    }


    protected static class DummyHeader extends Header {
        protected short num;

        public DummyHeader() {
        }

        public DummyHeader(short num) {
            this.num=num;
        }
        public short getMagicId() {return 1600;}
        public Supplier<? extends Header> create() {
            return DummyHeader::new;
        }

        public short getNum() {
            return num;
        }

        public int serializedSize() {
            return 0;
        }

        public void writeTo(DataOutput out) throws Exception {
        }

        public void readFrom(DataInput in) throws Exception {
        }

        public String toString() {
            return "DummyHeader(" + num + ")";
        }
    }

}
