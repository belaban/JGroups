
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.PingHeader;
import org.jgroups.protocols.TpHeader;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.util.Range;
import org.jgroups.util.UUID;
import org.jgroups.util.Util;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * Tests {@link BytesMessage} and {@link BaseMessage}
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class MessageTest extends MessageTestBase {
    protected static final String HELLO="hello world";

    public void testFlags() {
        Message m1=new BytesMessage();
        assert !m1.isFlagSet(Message.Flag.OOB);
        assert m1.getFlags(false) == 0;

        m1.setFlag((Message.Flag[])null);

        assert !m1.isFlagSet(Message.Flag.OOB);
        assert !m1.isFlagSet(Message.Flag.NO_RELIABILITY);
    }


    public void testSettingMultipleFlags() {
        Message msg=new BytesMessage();
        msg.setFlag((Message.Flag[])null);
        assert msg.getFlags(false) == 0;

        msg.setFlag(Message.Flag.OOB, Message.Flag.NO_FC, null, Message.Flag.DONT_BUNDLE);
        assert msg.isFlagSet(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.NO_FC);
        assert msg.isFlagSet(Message.Flag.DONT_BUNDLE);
    }


    public void testFlags2() {
        Message m1=new BytesMessage();
        m1.setFlag(Message.Flag.OOB);
        assert m1.isFlagSet(Message.Flag.OOB);
        assert Util.isFlagSet(m1.getFlags(false), Message.Flag.OOB);
        assert !(m1.isFlagSet(Message.Flag.DONT_BUNDLE));
        assert !Util.isFlagSet(m1.getFlags(false), Message.Flag.DONT_BUNDLE);
    }

    public void testFlags3() {
        Message msg=new BytesMessage();
        assert !msg.isFlagSet(Message.Flag.OOB);
        msg.setFlag(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.OOB);
        msg.setFlag(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.OOB);
    }


    public void testClearFlags() {
        Message msg=new BytesMessage();
        msg.setFlag(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.OOB);
        msg.clearFlag(Message.Flag.OOB);
        assert !msg.isFlagSet(Message.Flag.OOB);
        msg.clearFlag(Message.Flag.OOB);
        assert !msg.isFlagSet(Message.Flag.OOB);
        msg.setFlag(Message.Flag.OOB);
        assert msg.isFlagSet(Message.Flag.OOB);
    }


    public void testClearFlags2() {
        Message msg=new BytesMessage();
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
        assert msg.getFlags(false) == 0;
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
        Message msg=new EmptyMessage(null).setFlag(Message.TransientFlag.DONT_LOOPBACK);

        msg.setDest(null); // OK
        msg.setDest(null);
        msg.setDest(DEST);

        msg.clearFlag(Message.TransientFlag.DONT_LOOPBACK)
          .setDest(DEST) // OK
          .setFlag(Message.TransientFlag.DONT_LOOPBACK)
          .setFlagIfAbsent(Message.TransientFlag.DONT_LOOPBACK);

        short flags=(short)(Message.TransientFlag.DONT_LOOPBACK.value() + Message.TransientFlag.OOB_DELIVERED.value());
        msg.setFlag(flags, true);
    }


    public void testBufferSize() throws Exception {
        Message m1=new BytesMessage(null, "bela");
        assert m1.getArray() != null;
        assert m1.getArray() != null;
        Assert.assertEquals(m1.getArray().length, m1.getLength());
        byte[] new_buf={'m', 'i', 'c', 'h', 'e', 'l', 'l', 'e'};
        m1.setArray(new_buf, 0, new_buf.length);
        assert m1.getArray() != null;
        Assert.assertEquals(new_buf.length, m1.getLength());
        Assert.assertEquals(m1.getArray().length, m1.getLength());
    }


    public void testBufferOffset() throws Exception {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new BytesMessage(null, buf, 0, 4);
        Message m2=new BytesMessage(null, buf, 4, 3);

        byte[] b1, b2;

        b1=new byte[m1.getLength()];
        System.arraycopy(m1.getArray(), m1.getOffset(), b1, 0, m1.getLength());

        b2=new byte[m2.getLength()];
        System.arraycopy(m2.getArray(), m2.getOffset(), b2, 0, m2.getLength());

        Assert.assertEquals(4, b1.length);
        Assert.assertEquals(3, b2.length);
    }



    public void testSetBufferWithNullBuffer() {
        byte[] buf={'b', 'e', 'l', 'a'};
        Message m1=new BytesMessage();
        m1.setArray(buf, 1, 2); // dummy data with non 0 oiffset and length
        Assert.assertEquals(1, m1.getOffset());
        Assert.assertEquals(2, m1.getLength());

        m1.setArray(null, 1, 2); // dummy offset and length, is ignored
        Assert.assertEquals(0, m1.getOffset());
        Assert.assertEquals(0, m1.getLength());
    }


    @Test(groups=Global.FUNCTIONAL, expectedExceptions=ArrayIndexOutOfBoundsException.class)
    public void testInvalidOffset() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new BytesMessage(null, buf, -1, 4);
        System.out.println("message is " + m1);
    }

    @Test(groups=Global.FUNCTIONAL, expectedExceptions=ArrayIndexOutOfBoundsException.class)
    public void testInvalidLength() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new BytesMessage(null, buf, 3, 6);
        System.out.println("we should not get here with " + m1);
    }


    public void testGetRawBuffer() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new BytesMessage(null, buf, 0, 4);
        Message m2=new BytesMessage(null, buf, 4, 3);

        Assert.assertEquals(buf.length, m1.getArray().length);
        Assert.assertEquals(4, m1.getLength());

        Assert.assertEquals(buf.length, m2.getArray().length);
        Assert.assertEquals(3, m2.getLength());
    }


    public void testSetObject() {
        String s1="Bela Ban";
        Message m1=new BytesMessage(null, s1);
        Assert.assertEquals(0, m1.getOffset());
        Assert.assertEquals(m1.getArray().length, m1.getLength());
        String s2=m1.getObject();
        Assert.assertEquals(s2, s1);
    }

    public void testSetObjectWithByteBuffer() {
        byte[] H=HELLO.getBytes();
        Message msg=new BytesMessage(null, "bela".getBytes());
        ByteBuffer buf=ByteBuffer.wrap(H);
        msg.setObject(buf);
        assert msg.getLength() == H.length;
        assert msg.getOffset() == 0;
        byte[] pl=msg.getPayload();
        assert Arrays.equals(pl, H);

        buf=ByteBuffer.allocateDirect(H.length);
        buf.put(H).rewind();

        msg.setObject(buf);
        assert msg.getLength() == H.length;
        assert msg.getOffset() == 0;
        pl=msg.getPayload();
        assert Arrays.equals(pl, H);
    }


    public void testCopy() {
        Message m1=new BytesMessage(null, "Bela Ban")
          .setFlag(Message.Flag.OOB).setFlag(Message.TransientFlag.OOB_DELIVERED);
        Message m2=m1.copy(true, true);
        Assert.assertEquals(m1.getOffset(), m2.getOffset());
        Assert.assertEquals(m1.getLength(), m2.getLength());
        assert m2.isFlagSet(Message.Flag.OOB);
        assert m2.isFlagSet(Message.TransientFlag.OOB_DELIVERED);
    }

    public void testCopy2() {
        BytesMessage msg=new BytesMessage(null, "Bela".getBytes());
        Message copy=msg.copy(true, true);
        assert msg.getLength() == copy.getLength();
    }

    public void testCopyWithOffset() {
        byte[] buf={'b', 'e', 'l', 'a', 'b', 'a', 'n'};
        Message m1=new BytesMessage(null, buf, 0, 4);
        Message m2=new BytesMessage(null, buf, 4, 3);

        Message m3=m1.copy(true, true);
        Message m4=m2.copy(true, true);

        Assert.assertEquals(0, m3.getOffset());
        Assert.assertEquals(4, m3.getLength());

        Assert.assertEquals(4, m4.getOffset());
        Assert.assertEquals(3, m4.getLength());
    }

    public void testCopyHeaders() {
        Message m1=new BytesMessage(null, "hello");
        for(short id: new short[]{1, 2, 10, Global.BLOCKS_START_ID, Global.BLOCKS_START_ID +10}) {
            m1.putHeader(id, new DummyHeader(id));
        }
        System.out.println("Headers for m1: " + m1.printHeaders());

        Message m2=Util.copy(m1, true, Global.BLOCKS_START_ID, (short[])null);
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


    public void testSizeNullMessage() throws Exception {
        Message msg=new BytesMessage();
        assert !msg.hasPayload();
        assert msg.hasArray();
        _testSize(msg);
    }

    public void testSizeWithEmptyArray() {
        Message msg=new BytesMessage(null, new byte[0]);
        assert msg.hasPayload();
        assert msg.hasArray();
        assert msg.getLength() == 0;
    }


    public void testSizeMessageWithDest() throws Exception {
        Message msg=new EmptyMessage(UUID.randomUUID());
        _testSize(msg);
    }


    public void testSizeMessageWithSrc() throws Exception {
        Message msg=new EmptyMessage(null).setSrc(UUID.randomUUID());
        _testSize(msg);
    }


    public void testSizeMessageWithDestAndSrc() throws Exception {
        Message msg=new EmptyMessage(UUID.randomUUID()).setSrc(UUID.randomUUID());
        _testSize(msg);
    }



    public void testSizeMessageWithDestAndSrcAndFlags() throws Exception {
        Message msg=new EmptyMessage(UUID.randomUUID()).setSrc(UUID.randomUUID());
        msg.setFlag(Message.Flag.OOB);
        msg.setFlag(Message.Flag.DONT_BUNDLE);
        _testSize(msg);
    }


    public void testSizeMessageWithBuffer() throws Exception {
        Message msg=new BytesMessage(null, "bela".getBytes());
        _testSize(msg);
    }


    public void testSizeMessageWithBuffer2() throws Exception {
        Message msg=new BytesMessage(null, new byte[]{'b', 'e', 'l', 'a'});
        _testSize(msg);
    }


    public void testSizeMessageWithBuffer3() throws Exception {
        Message msg=new BytesMessage(null, "bela");
        _testSize(msg);
    }


    public void testSizeMessageWithDestAndSrcAndHeaders() throws Exception {
        Message msg=new BytesMessage(UUID.randomUUID(), "bela".getBytes()).setSrc(UUID.randomUUID());
        addHeaders(msg);
        _testSize(msg);
    }

    public void testMakeReply() {
        Address dest=Util.createRandomAddress("A"), src=Util.createRandomAddress("B");
        Message msg=new BytesMessage(dest, "Bela".getBytes()).setSrc(src);

        Message reply=makeReply(msg);
        System.out.println("reply = " + reply);
        assert Objects.equals(reply.getSrc(), msg.getDest());
        assert Objects.equals(reply.getDest(), msg.getSrc());
    }


    protected static void addHeaders(Message msg) {
        TpHeader tp_hdr=new TpHeader("DemoChannel2");
        msg.putHeader(UDP_ID, tp_hdr);
        PingHeader ping_hdr=new PingHeader(PingHeader.GET_MBRS_REQ).clusterName("demo-cluster");
        msg.putHeader(PING_ID, ping_hdr);
        NakAckHeader2 nak_hdr=NakAckHeader2.createXmitRequestHeader(Util.createRandomAddress("S"));
        msg.putHeader(NAKACK_ID, nak_hdr);
    }


    protected static void _testSize(Message msg) throws Exception {
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

        @Override
        public int serializedSize() {
            return 0;
        }

        @Override
        public void writeTo(DataOutput out) {
        }

        @Override
        public void readFrom(DataInput in) {
        }

        public String toString() {
            return "DummyHeader(" + num + ")";
        }
    }

}
